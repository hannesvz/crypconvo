import json
import boto3
import time
import base64
import random
import time
import os
import requests

endpoint_url = 'https://fcfcqpkf2a.execute-api.us-east-1.amazonaws.com/production'

def create_session(connectionid):
    try:
        ddb = boto3.client('dynamodb',region_name='us-east-1')
        res = ddb.put_item(
            TableName='crypconvo-sessions',
            Item={
                'id': {
                    'S': connectionid
                },
                'partner': {
                    'S': 'None'
                }
            })
    except Exception as e:
        print(e)


def delete_session(connectionid):
    try:
        ddb = boto3.client('dynamodb',region_name='us-east-1')
        res = ddb.delete_item(
            TableName='crypconvo-sessions',
            Key={
                'id': {
                    'S': connectionid
                }
            })
    except Exception as e:
        print(e)


def match_partner(connectionid1, connectionid2):
    try:
        ddb = boto3.client('dynamodb',region_name='us-east-1')
        res = ddb.update_item(
            TableName='crypconvo-sessions',
            Key={
                'id': {
                    'S': connectionid1
                }
            },
            ExpressionAttributeNames={
                '#key': 'partner'
            },
            ExpressionAttributeValues={
                ':val': {
                    'S': connectionid2
                }
            },
            UpdateExpression='SET #key = :val'
            )
        return True
    except Exception as e:
        print(f'something went wrong updating {connectionid1}')
        print(e)
        return False


def lookup_partner(connectionid):
    try:
        ddb = boto3.client('dynamodb',region_name='us-east-1')

        # look up who has the partner {connectionid}
        print(f'looking up who has the partner {connectionid}')
        res1 = ddb.query(
            TableName='crypconvo-sessions',
            IndexName='partner-index',
            ExpressionAttributeValues={
                ':partner': {
                    'S': connectionid,
                }
            },
            KeyConditionExpression='partner = :partner',
            ProjectionExpression='id',
            )
        partnerid = res1['Items'][0]['id']['S']
        print(f'partner found, id is {partnerid}')
    except Exception as e:
        print(f'something went wrong looking up the partner for {connectionid}')
        print(e)
        return None
    else:
        return partnerid


def disconnect_partner(connectionid):
    try:
        partnerid = lookup_partner(connectionid)
        ddb = boto3.client('dynamodb',region_name='us-east-1')
        # mark the client who has {connectionid} as a partner to no longer have that partner
        print(f'setting {partnerid} to no longer have a partner')
        res2 = ddb.update_item(
            TableName='crypconvo-sessions',
            Key={
                'id': {
                    'S': partnerid
                }
            },
            ExpressionAttributeNames={
                '#key': 'partner'
            },
            ExpressionAttributeValues={
                ':val': {
                    'S': 'None'
                }
            },
            UpdateExpression='SET #key = :val'
            )
        
        # send them a message notifying them their partner has disconnected
        matchup_message = {
            'type': 'partner_status',
            'action': 'partner_disconnected',
            'status': False
        }
        send_message(partnerid, matchup_message)
        return True
    except Exception as e:
        print(f'something went wrong updating {connectionid}')
        print(e)
        return False


def find_partner(connectionid):
    try:
        ddb = boto3.client('dynamodb',region_name='us-east-1')
        res = ddb.query(
            TableName='crypconvo-sessions',
            IndexName='partner-index',
            ExpressionAttributeValues={
                ':none': {
                    'S': 'None',
                }
            },
            KeyConditionExpression='partner = :none',
            ProjectionExpression='id',
            )
        # get the connection ids of all current clients
        connected_clients = list(map(lambda k: k['id']['S'], res['Items']))
        # filter out the connectionid of the one we're trying to find a match for
        eligible_partners = list(filter(lambda k: k != connectionid, connected_clients))
        print(f'eligible_partners: {eligible_partners}')
        
        matchup_message = {
            'type': 'partner_status',
            'status': True
        }
        
        if len(eligible_partners) > 0:
            matchup_message['action'] = 'partner_found'

            # an eligible partner was found to be online, update both parties
            partner = random.choice(eligible_partners)

            print(f'partner for {connectionid} found: {partner}')
            
            print(f'running match_partner for {connectionid} -> {partner}')
            match_partner(connectionid, partner)
            
            print(f'running match_partner for {partner} -> {connectionid}')
            match_partner(partner, connectionid)

            # let main connection know they have a matchup
            print(f'letting {connectionid} know their partner is {partner}')
            matchup_message['partnerid'] = partner
            send_message(connectionid, matchup_message)
            
            # let partner know they have a matchup
            print(f'letting {partner} know their partner is {connectionid}')
            matchup_message['partnerid'] = connectionid
            send_message(partner, matchup_message)
            
            return partner
        else:
            # no eligible partner was found, send back a response
            matchup_message['status'] = False
            matchup_message['action'] = 'partner_not_found'
            send_message(connectionid, matchup_message)
            return None
    except Exception as e:
        return None
        print(e)


def send_message(connectionid, data):
    bindata = json.dumps(data).encode()
    print(f'sending data {data} to {connectionid}')
    try:
        apigatewaymanagementapi = boto3.client('apigatewaymanagementapi',region_name='us-east-1',endpoint_url=endpoint_url)
        apigatewaymanagementapi.post_to_connection(
            Data=bindata,
            ConnectionId=connectionid
        )
    except Exception as e:
        print(e)


def pushbullet_message(message):
    token = os.environ['PUSHBULLET_TOKEN']
    post_body = {
        "type": "note",
        "title": "Push Message",
        "body": message
    }
    res = requests.post('https://api.pushbullet.com/v2/pushes', json=post_body, headers={'Access-Token':token})
    print(token)


def lambda_handler(event, context):
    if 'test' in event:
        pushbullet_message('test')
        return {
            'statusCode': 200,
            'body': json.dumps('OK')
        }
        
    try:
        routekey = event['requestContext']['routeKey']
        connectionid = event['requestContext']['connectionId']
        sourceip = event['requestContext']['identity']['sourceIp']
        if routekey == '$connect':
            create_session(connectionid)
            pushbullet_message('new connection on crypconvo!')

        if routekey == '$disconnect':
            delete_session(connectionid)
            print(f'disconnecting the partner for {connectionid}')
            disconnect_partner(connectionid)

        if routekey == '$default':
            body = json.loads(event['body'])

            # type: ping
            if body['type'] == 'ping':
                pong_message = {
                    'type': 'pong'
                }
                send_message(connectionid, pong_message)

            # type: whoami
            if body['type'] == 'whoami':
                connection_message = {'type':'connection_status','status':'connected','connectionid':connectionid}
                send_message(connectionid, connection_message)
            
            # type: connection_request
            if body['type'] == 'partner_request_msg':
                print(f'looking for a partner for {connectionid}')
                partner = find_partner(connectionid) # return data currently not really used here

            # type: message
            if body['type'] == 'message':
                print(f'message from {connectionid}: {body["message"]}')
                partnerid = lookup_partner(connectionid)
                if partnerid:
                    msg = {
                        'type': 'message',
                        'sender': connectionid,
                        'message': body['message']
                    }
                    send_message(connectionid, msg)
                    send_message(partnerid, msg)
            # type: manualdisconnect
            if body['type'] == 'disconnect':
                partnerid = lookup_partner(connectionid)
                disconnect_partner(connectionid)
                disconnect_partner(partnerid)
                # matchup_message = {
                #     'type': 'partner_status',
                #     'action': 'partner_disconnected',
                #     'status': False
                # }
                # send_message(partnerid, matchup_message)

    except Exception as e:
        print(e)
    
    return {
        'statusCode': 200,
        'body': json.dumps('OK')
    }
