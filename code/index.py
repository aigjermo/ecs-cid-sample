"""
AWS Lambda function to handle termination lifecycle events from EC2 autoscaling
groups used with ECS.
"""

from __future__ import print_function
from os import environ
import base64
import json
import datetime
import logging

import boto3


logging.basicConfig()
LOGGER = logging.getLogger()

if 'LOGDEBUG' in environ:
    LOGGER.setLevel(logging.DEBUG)
else:
    LOGGER.setLevel(logging.INFO)


# Establish boto3 session
SESSION = boto3.session.Session()
LOGGER.debug("Session is in region %s ", SESSION.region_name)

EC2CLIENT = SESSION.client(service_name='ec2')
ECSCLIENT = SESSION.client(service_name='ecs')
ASGCLIENT = SESSION.client('autoscaling')
SNSCLIENT = SESSION.client('sns')
LAMBDACLIENT = SESSION.client('lambda')


def getClusterName(ec2InstanceId):
    """
    Tries to get cluster name from the environment variable `ECS_CLUSTER`, and
    falls back to parsing the user data script of the container instance,
    returning the value of the `ECS_CLUSTER` field.
    """
    if 'ECS_CLUSTER' in environ:
        LOGGER.debug("Cluster name from env: %s", environ['ECS_CLUSTER'])
        return environ['ECS_CLUSTER']

    ec2Resp = EC2CLIENT.describe_instance_attribute(
        InstanceId=ec2InstanceId, Attribute='userData')
    LOGGER.debug("Describe instance attributes response %s", ec2Resp)
    userdata = base64.b64decode(ec2Resp['UserData']['Value'])

    for line in userdata.split():
        if line.find("ECS_CLUSTER") > -1:
            clusterName = line.split('=')[1]
            LOGGER.debug("Cluster name %s", clusterName)
            return clusterName

    LOGGER.debug("Cluster name not found in env or user data.")
    return None


def getContainerInstanceData(ec2InstanceId, clusterName):
    """
    Finds the container instance id from ECS that matches the instance id from
    EC2.
    """

    # Get list of container instance IDs from the clusterName
    paginator = ECSCLIENT.get_paginator('list_container_instances')

    for containerList in paginator.paginate(cluster=clusterName):

        containerDetResp = ECSCLIENT.describe_container_instances(
            cluster=clusterName,
            containerInstances=containerList['containerInstanceArns'])
        LOGGER.debug("describe container instances response %s",
                     containerDetResp)

        for instance in containerDetResp['containerInstances']:
            if instance['ec2InstanceId'] == ec2InstanceId:
                LOGGER.info("Container instance ID of interest : %s",
                            instance['containerInstanceArn'])
                return instance

    LOGGER.debug("Could not get ecs container instance data")
    return None


def getTerminatingLifeCycleHookName(message):
    """
    Retrieves the name of the termination life cycle hook that triggered this
    action
    """

    # If the event received is instance terminating...
    if 'LifecycleTransition' not in message.keys():
        LOGGER.debug("LifecycleTransition not in message")
        return None

    LOGGER.debug("message autoscaling %s", message['LifecycleTransition'])
    if message['LifecycleTransition'].find(
            'autoscaling:EC2_INSTANCE_TERMINATING') > -1:

        # Get lifecycle hook name
        lifecycleHookName = message['LifecycleHookName']
        LOGGER.debug("Setting lifecycle hook name %s ", lifecycleHookName)
        return lifecycleHookName

    return None


def publishToSNS(message, topicARN):
    """
    Publish SNS message to trigger lambda again.
        :param message:     To repost the complete original message received
                            when ASG terminating event was received.
        :param topicARN:    SNS topic to publish the message to.
    """
    LOGGER.info("Publish to SNS topic %s", topicARN)
    SNSCLIENT.publish(
        TopicArn=topicARN,
        Message=json.dumps(message),
        Subject='Publishing SNS message to invoke lambda again..'
    )
    return "published"


def instanceDrainingHandler(ec2InstanceId):
    """
    Check task status on the ECS container instance and manage draining.
        :param ec2InstanceId:   The EC2 instance ID is used to identify the
                                cluster and container instance to manage

        :returns                True if lifecycle should complete
    """
    clusterName = getClusterName(ec2InstanceId)
    instance = getContainerInstanceData(ec2InstanceId, clusterName)

    if instance is None:
        LOGGER.debug("Instance not found in ecs, probably not registered")
        return True

    instanceId = instance['containerInstanceArn']
    taskCount = instance['runningTasksCount']

    if instance['status'] != 'DRAINING':
        LOGGER.info("Draining instance: %s", instanceId)
        ECSCLIENT.update_container_instances_state(
            cluster=clusterName,
            containerInstances=[instanceId],
            status='DRAINING')
    else:
        LOGGER.debug("Instance is already draining")

    LOGGER.info("Remaining task count on %s: %d", instanceId, taskCount)
    return taskCount == 0


def lambdaHandler(event, _):
    """Lambda handler function"""
    LOGGER.info("Lambda received the event %s", event)

    message = json.loads(event['Records'][0]['Sns']['Message'])
    ec2InstanceId = message['EC2InstanceId']
    asgGroupName = message['AutoScalingGroupName']
    snsArn = event['Records'][0]['EventSubscriptionArn']
    topicArn = event['Records'][0]['Sns']['TopicArn']

    LOGGER.debug("records: %s", event['Records'][0])
    LOGGER.debug("sns: %s", event['Records'][0]['Sns'])
    LOGGER.debug("Message: %s", message)
    LOGGER.debug("Ec2 Instance Id: %s ,%s", ec2InstanceId, asgGroupName)
    LOGGER.debug("SNS ARN: %s", snsArn)
    LOGGER.debug("Topic ARN: %s", event['Records'][0]['Sns']['TopicArn'])

    lifecycleHookName = getTerminatingLifeCycleHookName(message)

    # If the event received is instance terminating...
    if lifecycleHookName is not None:
        # Check if there are any tasks running on the instance
        finished = instanceDrainingHandler(ec2InstanceId)

        if not finished:
            msgResponse = publishToSNS(message, topicArn)
            LOGGER.debug("msgResponse %s and time is %s",
                         msgResponse, datetime.datetime)

        else:
            LOGGER.debug(
                "Setting lifecycle to complete;No tasks are running on "
                "instance, completing lifecycle action....")

            try:
                response = ASGCLIENT.complete_lifecycle_action(
                    LifecycleHookName=lifecycleHookName,
                    AutoScalingGroupName=asgGroupName,
                    LifecycleActionResult='CONTINUE',
                    InstanceId=ec2InstanceId)
                LOGGER.debug(
                    "Response received from complete_lifecycle_action %s",
                    response)
                LOGGER.info("Completedlifecycle hook action")
            except Exception as err:
                print(str(err))
