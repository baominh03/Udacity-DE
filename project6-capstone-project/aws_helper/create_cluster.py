import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError
from time import time
from time import sleep


config_file_path = 'dwh_p.cfg'

config = configparser.ConfigParser()
config.read_file(open(config_file_path))


# read data from dwh.cfg file
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("CLUSTER", "DB_NAME")
DWH_DB_USER = config.get("CLUSTER", "DB_USER")
DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DWH_PORT = config.get("CLUSTER", "DB_PORT")

DELAY = int(config.get("DELAY", "DELAY_TIME"))
TIMEOUT = int(config.get("DELAY", "TIMEOUT"))

DWH_IAM_ROLE_NAME = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")


def create_clients():
    """
    Create clients for IAM, EC2, S3 and Redshift

    OUTPUTS:
    * ec2, s3, iam, redshift
    """
    ec2 = boto3.resource('ec2',
                         region_name="us-west-2",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                         )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    iam = boto3.client('iam', aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='us-west-2'
                       )

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
    print('CREATED CLIENTS')
    return ec2, s3, iam, redshift


def create_iam_role(iam):
    """
    Create iam role

    INPUTS:
    * iam client

    OUTPUTS:
    * roleArn (e.g: arn:aws:iam::988332130976:role/dwhRole)
    """
    # 1.1 Create the role,
    try:
        print("1.1 Creating a new IAM Role")
        iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    role_arn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print('roleArn: [{}]'.format(role_arn))

    return role_arn


def create_cluster(role_arn, redshift):
    """
    Create Redshift CLuster

    INPUTS: 
    * role_arn
    * redshift client

    OUTPUTS:
    * my_cluster_props: redshift props
    * DWH_ENDPOINT: Data warehouse endpoint
    * DWH_ROLE_ARN: Data warehouse role arn
    """
    print('=== Create redshift cluster ===')
    try:
        redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[role_arn]
        )
    except Exception as e:
        print(e)
    sleep(2)
    t0 = time()
    count = 1
    cluster_props = redshift.describe_clusters(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    cluster_status = cluster_props['ClusterStatus']
    if cluster_status == 'available':
        DWH_ENDPOINT = cluster_props['Endpoint']['Address']
        DWH_ROLE_ARN = cluster_props['IamRoles'][0]['IamRoleArn']
        print('Redshift host available: ' +  DWH_ENDPOINT)
        return DWH_ENDPOINT, DWH_ROLE_ARN, cluster_props
    while cluster_status == 'creating':
        print("ClusterStatus[{}]: [{}]\n".format(count, cluster_status))
        print('delay {} sec(s)\n'.format(str(DELAY)))
        sleep(DELAY)
        count = count + 1
        my_cluster_props = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        cluster_status = my_cluster_props['ClusterStatus']
        if time()-t0 > TIMEOUT:
            raise ValueError(
                "Redshift creation time is too long, please double check to avoid wasting money in: {0:.2f} sec\n".format(time()-t0))
    print("=== REDSHIFT CLUSTER CREATED in: {0:.2f} sec\n".format(time()-t0))
    print("ClusterStatus: [{}]\n".format(cluster_status))
    DWH_ENDPOINT = my_cluster_props['Endpoint']['Address']
    DWH_ROLE_ARN = my_cluster_props['IamRoles'][0]['IamRoleArn']
    print('Redshift host: ' +  DWH_ENDPOINT)
    return DWH_ENDPOINT, DWH_ROLE_ARN, my_cluster_props


def open_incoming_tcp_port(my_cluster_props, ec2):
    """
    To open incoming tcp port on EC2

    INPUTS: 
    * my_cluster_props
    * ec2 client

    """
    try:
        vpc = ec2.Vpc(id=my_cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )

    except Exception as e:
        print(e)


if __name__ == "__main__":
    ec2, s3, iam, redshift = create_clients()
    role_arn = create_iam_role(iam)
    DWH_ENDPOINT, DWH_ROLE_ARN, my_cluster_props = create_cluster(
        role_arn, redshift)
    open_incoming_tcp_port(my_cluster_props, ec2)

    # write HOST,  DWH_ROLE_ARN to dwh.cfg
    config.read(config_file_path)
    config.set('CLUSTER', 'HOST', DWH_ENDPOINT)
    config.set('IAM_ROLE', 'DWH_ROLE_ARN', DWH_ROLE_ARN)
    with open(config_file_path, 'w') as configfile:
        config.write(configfile)
