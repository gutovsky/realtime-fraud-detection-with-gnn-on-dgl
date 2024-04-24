import * as path from 'path';
import {
  HttpApi,
  HttpIntegrationType,
  HttpConnectionType,
  PayloadFormatVersion,
  HttpMethod,
  HttpStage,
} from '@aws-cdk/aws-apigatewayv2-alpha';
import { HttpLambdaIntegration } from '@aws-cdk/aws-apigatewayv2-integrations-alpha';
import {
  GraphqlApi,
  Schema,
  MappingTemplate,
  FieldLogLevel,
  AuthorizationType,
} from '@aws-cdk/aws-appsync-alpha';
import { PythonFunction } from '@aws-cdk/aws-lambda-python-alpha';
import {
  NestedStack,
  NestedStackProps,
  RemovalPolicy,
  CfnOutput,
  Duration,
  CustomResource,
  CfnMapping,
  Aws,
  Fn,
  Resource,
  Stack,
  Arn,
  CfnResource,
  Aspects,
  ArnFormat,
} from 'aws-cdk-lib';
import {
  CfnIntegration,
  CfnRoute,
  CfnStage,
} from 'aws-cdk-lib/aws-apigatewayv2';
import {
  IVpc,
  SubnetType,
  InstanceType,
  InstanceClass,
  InstanceSize,
  SecurityGroup,
  Port,
} from 'aws-cdk-lib/aws-ec2';
import {
  Role,
  ServicePrincipal,
  PolicyDocument,
  PolicyStatement,
  ArnPrincipal,
  ManagedPolicy,
} from 'aws-cdk-lib/aws-iam';
import { Key } from 'aws-cdk-lib/aws-kms';
import { LayerVersion, Code, Runtime, Tracing } from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { RetentionDays, LogGroup } from 'aws-cdk-lib/aws-logs';
import { IHostedZone, ARecord, RecordTarget } from 'aws-cdk-lib/aws-route53';
import { CloudFrontTarget } from 'aws-cdk-lib/aws-route53-targets';
import { Bucket, BucketEncryption, BlockPublicAccess, IBucket } from 'aws-cdk-lib/aws-s3';
import {
  BucketDeployment,
  Source,
  CacheControl,
  StorageClass,
} from 'aws-cdk-lib/aws-s3-deployment';
import { IQueue } from 'aws-cdk-lib/aws-sqs';
import {
  IntegrationPattern,
  StateMachine,
  LogLevel,
  Map as SfnMap,
  Errors,
  Pass,
  DISCARD,
  JsonPath,
} from 'aws-cdk-lib/aws-stepfunctions';
import { LambdaInvoke } from 'aws-cdk-lib/aws-stepfunctions-tasks';
import {
  Provider,
  AwsCustomResource,
  PhysicalResourceId,
  AwsCustomResourcePolicy,
} from 'aws-cdk-lib/custom-resources';
import { Construct } from 'constructs';
import { IEEE, getDatasetMapping } from './dataset';
import { WranglerLayer } from './layer';
import { SARDeployment } from './sar';
import { artifactsHash, CfnNagWhitelist, grantKmsKeyPerm } from './utils';

export interface TransactionDashboardStackStackProps extends NestedStackProps {
  readonly vpc: IVpc;
  readonly queue: IQueue;
  readonly inferenceArn: string;
  readonly accessLogBucket: IBucket;
  readonly customDomain?: string;
  readonly r53HostZoneId?: string;
}

export class TransactionDashboardStack extends NestedStack {

  // readonly distribution: IDistribution;

  constructor(
    scope: Construct,
    id: string,
    props: TransactionDashboardStackStackProps,
  ) {
    super(scope, id, props);

    const kmsKey = new Key(this, 'realtime-fraud-detection-with-gnn-on-dgl-dashboard', {
      alias: 'realtime-fraud-detection-with-gnn-on-dgl/dashboard',
      enableKeyRotation: true,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const wranglerLayer = new WranglerLayer(this, 'AwsDataWranglerLayer')

    const simEnd = new Pass(this, 'Stop generation');

    const tranSimFn = new PythonFunction(this, 'TransactionSimulatorFunc', {
      entry: path.join(__dirname, '../lambda.d/simulator'),
      layers: [
        wranglerLayer
      ],
      index: 'gen.py',
      runtime: Runtime.PYTHON_3_9,
      environment: {
        INFERENCE_ARN: props.inferenceArn,
        DATASET_URL: getDatasetMapping(this).findInMap(Aws.PARTITION, IEEE),
      },
      timeout: Duration.minutes(15),
      memorySize: 3008,
      tracing: Tracing.ACTIVE,
    });
    tranSimFn.addToRolePolicy(new PolicyStatement({
      actions: ['lambda:InvokeFunction'],
      resources: [props.inferenceArn],
    }),
    );
    const tranSimTask = new (class extends LambdaInvoke {
      public toStateJson(): object {
        return {
          ...super.toStateJson(),
          TimeoutSecondsPath: '$.duration',
        };
      }
    })(this, 'Generate live transactions', {
      lambdaFunction: tranSimFn,
      integrationPattern: IntegrationPattern.REQUEST_RESPONSE,
    }).addCatch(simEnd, {
      errors: [Errors.TIMEOUT],
      resultPath: JsonPath.DISCARD,
    });

    const map = new SfnMap(this, 'Concurrent simulation', {
      inputPath: '$.parameters',
      itemsPath: '$.iter',
      maxConcurrency: 0,
    });
    map.iterator(tranSimTask);

    const paraFn = new NodejsFunction(this, 'ParametersFunc', {
      entry: path.join(__dirname, '../lambda.d/simulator/parameter.ts'),
      handler: 'iter',
      timeout: Duration.seconds(30),
      memorySize: 128,
      runtime: Runtime.NODEJS_16_X,
      tracing: Tracing.ACTIVE,
    });
    const parameterTask = new (class extends LambdaInvoke {
      public toStateJson(): object {
        return {
          ...super.toStateJson(),
          ResultSelector: {
            'parameters.$': '$.Payload',
          },
        };
      }
    })(this, 'Simulation prepare', {
      lambdaFunction: paraFn,
      integrationPattern: IntegrationPattern.REQUEST_RESPONSE,
    });

    const definition = parameterTask.next(map);
    const genLogGroupName = `/aws/vendedlogs/realtime-fraud-detection-with-gnn-on-dgl/dashboard/simulator/${this.stackName}`;
    grantKmsKeyPerm(kmsKey, genLogGroupName);
    const transactionGenerator = new StateMachine(
      this,
      'TransactionGenerator',
      {
        definition,
        logs: {
          destination: new LogGroup(this, 'FraudDetectionSimulatorLogGroup', {
            encryptionKey: kmsKey,
            retention: RetentionDays.SIX_MONTHS,
            logGroupName: genLogGroupName,
            removalPolicy: RemovalPolicy.DESTROY,
          }),
          includeExecutionData: true,
          level: LogLevel.ALL,
        },
        tracingEnabled: true,
      },
    );
    (transactionGenerator.node.findChild('Role').node.findChild('DefaultPolicy')
      .node.defaultChild as CfnResource).addMetadata('cfn_nag', {
      rules_to_suppress: [
        {
          id: 'W12',
          reason: 'wildcard in policy is used for x-ray and logs',
        },
      ],
    });

    const httpApi = new HttpApi(this, 'FraudDetectionDashboardApi', {
      createDefaultStage: false,
    });
    const apiRole = new Role(this, 'FraudDetectionDashboardApiRole', {
      assumedBy: new ServicePrincipal('apigateway.amazonaws.com'),
    });
    const generatorStartIntegration = new CfnIntegration(
      this,
      'GeneratorStartIntegration',
      {
        apiId: httpApi.httpApiId,
        integrationType: HttpIntegrationType.AWS_PROXY,
        integrationSubtype: 'StepFunctions-StartExecution',
        connectionType: HttpConnectionType.INTERNET,
        credentialsArn: apiRole.roleArn,
        description:
          'integrate with the generator implemented by step functions',
        payloadFormatVersion: PayloadFormatVersion.VERSION_1_0.version,
        requestParameters: {
          StateMachineArn: transactionGenerator.stateMachineArn,
          Input: '$request.body.input',
        },
        timeoutInMillis: 1000 * 10,
      },
    );
    const generatorPath = '/start';
    new CfnRoute(this, 'GeneratorRoute', {
      apiId: httpApi.httpApiId,
      routeKey: `${HttpMethod.POST} ${generatorPath}`,
      authorizationType: 'NONE',
      target: `integrations/${generatorStartIntegration.ref}`,
    });
    transactionGenerator.grantStartExecution(apiRole);

    // lambda that knows to receive HTTP post request and format it as an event
    const RequestFormatterFn = new PythonFunction(this, 'RequestFormatterFunc', {
      entry: path.join(__dirname, '../lambda.d/formatter'),
      layers: [
        wranglerLayer
      ],
      environment: {
        INFERENCE_ARN: props.inferenceArn,
      },
      index: "request_formatter.py",
      runtime: Runtime.PYTHON_3_9,
      timeout: Duration.seconds(60),
      memorySize: 3008,
      tracing: Tracing.ACTIVE,
    })
    RequestFormatterFn.addToRolePolicy(new PolicyStatement({
      actions: ['lambda:InvokeFunction'],
      resources: [props.inferenceArn],
    }),
    );
     
    // integration for single requests handling
    const singleRequestsHandlingIntegration = new HttpLambdaIntegration(
      "SingleRequestsIntegration",
      RequestFormatterFn
    )

    const singleRequestsPath = '/request';
    httpApi.addRoutes({
      path: singleRequestsPath,
      methods: [HttpMethod.POST],
      integration: singleRequestsHandlingIntegration,
    })
  }
}
