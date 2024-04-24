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
  CompositePrincipal,
  Effect,
  Policy,
} from 'aws-cdk-lib/aws-iam';
import { Key } from 'aws-cdk-lib/aws-kms';
import { LayerVersion, Code, Runtime, Tracing } from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { RetentionDays, LogGroup } from 'aws-cdk-lib/aws-logs';
import { IHostedZone, ARecord, RecordTarget } from 'aws-cdk-lib/aws-route53';
import { CloudFrontTarget } from 'aws-cdk-lib/aws-route53-targets';
import { Bucket, BucketEncryption, BlockPublicAccess, IBucket, EventType } from 'aws-cdk-lib/aws-s3';
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
import { CfnDeliveryStream } from 'aws-cdk-lib/aws-kinesisfirehose';
import { CfnCrawler } from 'aws-cdk-lib/aws-glue';
import { DataFormat, Database, Table, Schema } from '@aws-cdk/aws-glue-alpha';
import * as athena from 'aws-cdk-lib/aws-athena'
import { LambdaDestination } from 'aws-cdk-lib/aws-s3-notifications';
import { StringParameter } from 'aws-cdk-lib/aws-ssm';
import { CfnDataSet, CfnDataSource } from 'aws-cdk-lib/aws-quicksight';

export interface AnalyticsStackProps extends NestedStackProps {
    readonly vpc: IVpc;
    readonly queue: IQueue;
}

export class AnalyticsStack extends NestedStack {

  constructor(
    scope: Construct,
    id: string,
    props: AnalyticsStackProps,
  ) {
    super(scope, id, props);

    const transaction_bucket = new Bucket(this, 'FirehoseBucket', {
        removalPolicy: RemovalPolicy.DESTROY,
      });

    const firehoseRole = new Role(this, 'FirehoseDeliveryRole', {
        assumedBy: new ServicePrincipal('firehose.amazonaws.com'),
    });

    transaction_bucket.grantReadWrite(firehoseRole);

     const firehoseStream = new CfnDeliveryStream(this, 'FraudDetectionAnalyticsFirehose', {
        deliveryStreamType: 'DirectPut',
        s3DestinationConfiguration: {
          bucketArn: transaction_bucket.bucketArn,
          roleArn: firehoseRole.roleArn,
          bufferingHints: {
            sizeInMBs: 1,
          },
          compressionFormat: 'UNCOMPRESSED',
        }
      });

    // lambda that pushes data from the queue to Kinesis
    const tranEventFn = new PythonFunction(this, 'TransactionEventFunc', {
        entry: path.join(__dirname, '../lambda.d/analytics'),
        layers: [
          new WranglerLayer(this, 'AwsDataWranglerLayer')
        ],
        environment: {
            STREAM_NAME: firehoseStream.ref,
        },
        index: "event.py",
        runtime: Runtime.PYTHON_3_9,
        timeout: Duration.seconds(60),
        memorySize: 3008,
        tracing: Tracing.ACTIVE,
      });
      
    tranEventFn.addEventSource(
        new SqsEventSource(props.queue, {
            batchSize: 10,
            enabled: true,
        }),
    );

    tranEventFn.addToRolePolicy(new PolicyStatement({
        actions: ['firehose:PutRecord'],
        resources: [firehoseStream.attrArn],
        }),
    );

    const transactionDatabase = new Database(this, 'FraudDetectionDatabase', {
        databaseName: 'fraud_detection_analytics_db',
      });
  
    const transactionTable = new Table(this, 'TransactionTable', {
      database: transactionDatabase,
      tableName: 'transaction_for_analytics',
      description: 'Transaction Table',
      columns: [
        { name: 'id', type: Schema.STRING },
      ],
      dataFormat: DataFormat.PARQUET,
      bucket: transaction_bucket,
      storedAsSubDirectories: true,
    });

    // create crawler to update tables
    const crawlerRole = new Role(this, 'DataCrawlerRole', {
      assumedBy: new CompositePrincipal(
        new ServicePrincipal('glue.amazonaws.com')),
      managedPolicies: [ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')],
    });
    transaction_bucket.grantRead(crawlerRole);

    const crawler = new CfnCrawler(this, 'DataCrawler', {
      role: crawlerRole.roleArn,
      targets: {
        catalogTargets: [{
          databaseName: transactionDatabase.databaseName,
          tables: [
            transactionTable.tableName,
          ],
        }],
      },
      databaseName: transactionDatabase.databaseName,
      description: 'The crawler updates tables in Data Catalog for fraud detection analytics.',
      schemaChangePolicy: {
        updateBehavior: 'UPDATE_IN_DATABASE',
        deleteBehavior: 'LOG',
      },
      configuration: JSON.stringify({
        "Version": 1.0,
        "CrawlerOutput": {
          "Partitions": {
            "AddOrUpdateBehavior": "InheritFromTable"
          }
        },
        "Grouping": {
          "TableGroupingPolicy": "CombineCompatibleSchemas"
        }
      })
    });

    const dataCatalogCrawlerFn = new NodejsFunction(this, 'AnalyticsDataCatalogCrawler', {
      entry: path.join(__dirname, '../lambda.d/crawl-data-catalog-analytics/index.ts'),
      handler: 'crawler',
      timeout: Duration.minutes(15),
      memorySize: 128,
      runtime: Runtime.NODEJS_16_X,
      tracing: Tracing.ACTIVE,
      environment: {
        CrawlerName: crawler.ref,
      }
    });
    transaction_bucket.addEventNotification(EventType.OBJECT_CREATED, new LambdaDestination(dataCatalogCrawlerFn));

    const gluePolicy = new Policy(this, 'analyticsGluePolicy', {
      statements: [
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'glue:StartCrawler',
          ],
          resources: [Arn.format({
            service: 'glue',
            resource: 'crawler',
            resourceName: crawler.ref,
          }, Stack.of(this))],
        }),
        new PolicyStatement({
          effect: Effect.ALLOW,
          actions: [
            'glue:GetCrawlerMetrics',
          ],
          resources: ['*'],
        }),
      ],
    });
    (gluePolicy.node.defaultChild as CfnResource)
      .addMetadata('cfn_nag', {
        rules_to_suppress: [
          {
            id: 'W12',
            reason: 'wildcard resource for glue:GetCrawlerMetrics is intended',
          },
        ],
      });
    dataCatalogCrawlerFn.role?.attachInlinePolicy(gluePolicy);

    // Athena
    const queryResultBucket = new Bucket(this, `queryResultsBucket`, {
      bucketName: `fraud-detection-query-results-all-workgroups`,
      lifecycleRules: [{ expiration: Duration.days(30) }],
    })

    const workGroupNames: string[] = ['dashboard', 'metadata', 'usage']
    const workGroupObjects: { [key: string]: athena.CfnWorkGroup } = Object.assign(
      {},
      ...workGroupNames.map((workGroupName) => {
          return {
            [workGroupName]: this.createWorkGroup(workGroupName, this, queryResultBucket),
          }
      }),
    )

    // Quicksight
    const quickSightRole = new Role(this, 'QuickSightAthenaRole', {
      assumedBy: new ServicePrincipal('quicksight.amazonaws.com'),
    });

    // Add permissions to the QuickSight role
    quickSightRole.addToPolicy(new PolicyStatement({
      actions: [
        "athena:GetDataCatalog",
        "athena:GetNamedQuery",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:GetWorkGroup",
        "athena:ListDataCatalogs",
        "athena:ListNamedQueries",
        "athena:ListQueryExecutions",
        "athena:ListTagsForResource",
        "athena:ListWorkGroups",
        "athena:StartQueryExecution",
        "athena:StopQueryExecution",
        "athena:TagResource",
        "athena:UntagResource",
        "athena:UpdateDataCatalog",
        "athena:UpdateNamedQuery",
        "athena:UpdateWorkGroup",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:PutObject",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables"
      ],
      resources: ['*'],
    }));

    // // Create a QuickSight data source for Athena
    // const dataSource = new CfnDataSource(this, 'AthenaDataSource', {
    //   awsAccountId: '851725304354',
    //   dataSourceId: 'athena-datasource',
    //   name: 'Athena DataSource',
    //   type: 'ATHENA',
    //   dataSourceParameters: {
    //     athenaParameters: {
    //       workGroup: `athenaWorkGroup-dashboard`,
    //     },
    //   },
    //   permissions: [{
    //     principal: quickSightRole.roleArn,
    //     actions: ["quicksight:DescribeDataSource", "quicksight:DescribeDataSourcePermissions", "quicksight:PassDataSource", "quicksight:UpdateDataSource", "quicksight:DeleteDataSource", "quicksight:UpdateDataSourcePermissions"],
    //   }],
    //   sslProperties: {
    //     disableSsl: false,
    //   },
    // });

    // // (Optional) Define a QuickSight dataset linked to the data source
    // const dataSet = new CfnDataSet(this, 'AthenaDataSet', {
    //   awsAccountId: '851725304354',
    //   dataSetId: 'athena-dataset',
    //   name: 'Athena Dataset',
    //   physicalTableMap: {
    //     'transactions': {
    //       relationalTable: {
    //         dataSourceArn: dataSource.attrArn,
    //         catalog: 'AwsDataCatalog',
    //         schema: transactionDatabase.databaseName,
    //         name: transactionTable.tableName,
    //         inputColumns: [{ name: 'column1', type: 'STRING' }, { name: 'column2', type: 'INTEGER' }],
    //       },
    //     },
    //   },
    //   permissions: [{
    //     principal: quickSightRole.roleArn,
    //     actions: ["quicksight:DescribeDataSet", "quicksight:DescribeDataSetPermissions", "quicksight:PassDataSet", "quicksight:UpdateDataSet", "quicksight:DeleteDataSet", "quicksight:UpdateDataSetPermissions"],
    //   }],
    // });

      
  }

  private createWorkGroup(workGroupName: string, stack: AnalyticsStack, bucket: Bucket): athena.CfnWorkGroup {
    const workGroup = new athena.CfnWorkGroup(stack, `athenaWorkGroup-${workGroupName}`, {
      name: `athenaWorkGroup-${workGroupName}`,
      workGroupConfiguration: {
          resultConfiguration: {
              outputLocation: `${bucket.s3UrlForObject()}/${workGroupName}`,
          },
      },
    })
    return workGroup
  }
}
