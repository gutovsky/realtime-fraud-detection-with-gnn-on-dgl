FROM public.ecr.aws/docker/library/alpine:3.18
CMD ["sh", "-c", "mkdir -p /asset-output/ && wget -qO- https://github.com/awslabs/aws-data-wrangler/releases/download/2.16.1/awswrangler-layer-2.16.1-py3.9.zip | unzip - -d /asset-output"]
EXPOSE 3000