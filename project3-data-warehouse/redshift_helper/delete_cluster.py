import boto3
import configparser
from time import time
from time import sleep

# Run local
config_file_path = './project3-data-warehouse/dwh.cfg'
# Run on Udacity workspace
# config_file_path = 'dwh.cfg'

config = configparser.ConfigParser()
config.read_file(open(config_file_path))

KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
DWH_CLUSTER_IDENTIFIER = config.get("DWH", "dwh_cluster_identifier")
DWH_IAM_ROLE_NAME = config.get("IAM_ROLE", "DWH_IAM_ROLE_NAME")

DELAY = int(config.get("DELAY", "DELAY_TIME"))
TIMEOUT = int(config.get("DELAY", "TIMEOUT"))


def create_clients():
    """
    Create clients for IAM and Redshift

    OUTPUTS:
    * iam, redshift
    """
    iam = boto3.client('iam', aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='us-west-2'
                       )

    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
    print('CREATED CLIENTS IAM & REDSHIFT FOR DELETING')
    return iam, redshift


def delete_cluster(redshift):
    """
    Delete redshift cluster

    INPUTS:
    * redshift clients
    """
    redshift.delete_cluster(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    print('Sent request: delete cluster')
    try:
        sleep(2)
        t0 = time()
        count = 1
        cluster_status = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']
        while cluster_status == 'deleting':
            print("ClusterStatus[{}]: [{}]\n".format(count, cluster_status))
            print('delay {} sec(s)\n'.format(str(DELAY)))
            sleep(DELAY)
            count = count + 1
            my_cluster_props = redshift.describe_clusters(
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            cluster_status = my_cluster_props['ClusterStatus']
            if time()-t0 > TIMEOUT:
                raise ValueError(
                    "Redshift termination time is too long, please double check to avoid wasting money in: {0:.2f} sec\n".format(time()-t0))
    except redshift.exceptions.ClusterNotFoundFault as e:
        print(e)
        print(
            "=== REDSHIFT CLUSTER REMOVED in: {0:.2f} sec\n".format(time()-t0))


def delete_iam_role(iam):
    """
    Delete iam role

    INPUTS:
    * iam clients
    """
    try:
        iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
        print("=== IAM ROLE REMOVED\n")
    except Exception as e:
        print(e)

if __name__ == "__main__":

    # To clear value of HOST and DWH_ROLE_ARN, load_time_nodist, load_time_dist in dwh.cfg file
    config.read(config_file_path)
    config.set('CLUSTER', 'HOST', '<autofill after running create_cluster.py>')
    config.set('IAM_ROLE', 'DWH_ROLE_ARN', '<autofill after running create_cluster.py>')

    config.set('NODIST', 'LOAD_TIME_STAGING_EVENTS', '<autofill after running etl.py>')
    config.set('NODIST', 'LOAD_TIME_STAGING_SONGS', '<autofill after running etl.py>')
    config.set('DIST', 'LOAD_TIME_STAGING_EVENTS', '<autofill after running etl.py>')
    config.set('DIST', 'LOAD_TIME_STAGING_SONGS', '<autofill after running etl.py>')
    with open(config_file_path, 'w') as configfile:
        config.write(configfile)
        
    iam, redshift = create_clients()
    delete_cluster(redshift)
    delete_iam_role(iam)
