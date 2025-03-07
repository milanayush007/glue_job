import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import uuid
import boto3
import json
import time
import requests
from urllib.parse import urlencode
from datetime import datetime
from pyspark.sql import DataFrame, Row
from awsglue import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection
from pyspark.sql.functions import col, from_json, to_timestamp, to_json
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType
from botocore.exceptions import BotoCoreError, ClientError
from boto3.dynamodb.types import TypeDeserializer
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('data-migration-mapping')
BASE_S3_PATH = "s3://infin8-data-migration"



# ----process-record-------------------------------------------------------

def processRecord(row):
    try:

        dynamodb_json = json.loads(row)
        deserializer = TypeDeserializer()
        item = {k: deserializer.deserialize(v) for k, v in dynamodb_json.items()}
        cleaned_item = {k: None if (v is None or v in ("null", "NULL", "")) else v for k, v in item.items()}
        return cleaned_item
    except Exception as e:
        print(f"Error deserializing DynamoDB JSON: {str(e)}")
        return None


# ----execute-api-------------------------------------------------------

def executeApi(api_config, token,  max_retries=2):
    for attempt in range(max_retries):
        try:

            response = requests.post(
                api_config["url"], 
                json=api_config["payload"], 
                headers=api_config["headers"]
            )
            print(f"Response status code: {response.status_code}")
            print(f"Response body: {response.text}")
            
            # Handle token expiration
            if response.status_code == 401 and attempt < max_retries - 1:
                print("Token expired. Generating new token...")
                new_token = get_auth_token(args['auth_url'], args['client_id'], 
                                         args['username'], args['password'], 
                                         args['client_secret'])
                if not new_token:
                    return {'isSuccess': False, 'error': 'Failed to obtain new auth token'}, None, api_config["payload"]
                token = new_token
                api_config["headers"]['Authorization'] = token
                time.sleep(1)
                continue
            
            response.raise_for_status()
            result = process_response(response.json())
            return result, token, api_config["payload"]
            
        except requests.exceptions.RequestException as e:
            print(e)
            # if response.status_code != 401 or attempt == max_retries - 1:
            return {'isSuccess': False, 'error': str(e)}, token, api_config["payload"]
    
    return {'isSuccess': False, 'error': 'Max retries reached'}, token, api_config["payload"]


# ----update-mapping-------------------------------------------------------

def updateMapping(result, item, sent_payload, sk_id = None):
    """
    Processes the API result and updates DynamoDB accordingly
    """
    try:
        pk = item.get('pk')
        if pk == "contact":
            sk_base = str(item.get('email_id'))
        else:
            sk_base = sk_id or str(item.get('sk'))

        content_json = json.dumps(sent_payload)
        
        if result['isSuccess']:
            api_response = result.get('response', {})
            if new_id := get_v2_id(api_response):
                success = create_dynamo_record(
                    table=table,
                    pk=pk,
                    sk=f"success#{sk_base}",
                    v1_value=sk_base,
                    result='success',
                    payload=content_json,
                    v2_value=new_id,
                    response=api_response
                )
                if success:
                    print(f"Successfully processed record with v2_id: {new_id}")
            else:
                print("No v2_id found in successful response")
        else:
            create_dynamo_record(
                table=table,
                pk=pk,
                sk=f"failed#{sk_base}",
                v1_value=sk_base,
                result='failed',
                payload=content_json,
                error_msg=str(result.get('error', 'Unknown error')),
                response=result.get('response', {})
            )
            print(f"API call failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"Error processing API result: {str(e)}")


# ----process-bulk-response-------------------------------------------------------   
def exectueBulkAPI(api_config, token,  max_retries=2):
    for attempt in range(max_retries):
        try:

            response = requests.post(
                api_config["url"], 
                json=api_config["payload"], 
                headers=api_config["headers"]
            )
            print(f"Response status code: {response.status_code}")
            print(f"Response body: {response.text}")
            
            # Handle token expiration
            if response.status_code == 401 and attempt < max_retries - 1:
                print("Token expired. Generating new token...")
                new_token = get_auth_token(args['auth_url'], args['client_id'], 
                                         args['username'], args['password'], 
                                         args['client_secret'])
                if not new_token:
                    return {'isSuccess': False, 'error': 'Failed to obtain new auth token'}, None, api_config["payload"]
                token = new_token
                api_config["headers"]['Authorization'] = token
                time.sleep(1)
                continue
            
            response.raise_for_status()
            result = process_bulk_response(response.json())
            return result, token, api_config["payload"]
            
        except requests.exceptions.RequestException as e:
            print(e)
            # if response.status_code != 401 or attempt == max_retries - 1:
            return {'isSuccess': False, 'error': str(e)}, token, api_config["payload"]
    
    return {'isSuccess': False, 'error': 'Max retries reached'}, token, api_config["payload"]



def updateMappingForBulkResponses(response_json, item, sent_payload, sk_id):
    """
    Processes the bulk API result and updates DynamoDB accordingly.
    """
    try:
        if 'result' in response_json:
            results = response_json['result']
            for result in results:
                status_code = result.get('statusCode')
                is_success = status_code == 200
                pk = item.get('pk')
                sk_base = str(item.get('sk'))
                content_json = json.dumps(sent_payload)

                if is_success:
                    api_response = result.get('response', {})
                    if new_id := get_v2_id(api_response):
                        success = create_dynamo_record(
                            table=table,
                            pk=pk,
                            sk=f"success#{sk_id}#{uuid.uuid4()}",
                            v1_value=sk_base,
                            result='success',
                            payload=content_json,
                            v2_value=new_id,
                            response=api_response
                        )
                        if success:
                            print(f"Successfully processed record with v2_id: {new_id}")
                    else:
                        print("No v2_id found in successful response")
                else:
                    create_dynamo_record(
                        table=table,
                        pk=pk,
                        sk=f"failed#{sk_base}#{sub_entity}#{uuid.uuid4()}",
                        v1_value=sk_base,
                        result='failed',
                        payload=content_json,
                        error_msg=str(result.get('error', 'Unknown error')),
                        response=result.get('response', {})
                    )
                    print(f"API call failed: {result.get('error', 'Unknown error')}")
        else:
            print("Invalid response format")
    except Exception as e:
        print(f"Error processing bulk API result: {str(e)}")


# ----payload creation functions-------------------------------------------------------

def create_organization_payload(record, token):
    """Creates payload, URL and headers for organization entity"""

    processed_payload = filter_payload_keys(record, "pk", "sk", "id")
    if "organization_is_active" in processed_payload:
        processed_payload["organization_is_active"] = convert_to_boolean(processed_payload["organization_is_active"])

    if "finmoId" in processed_payload:
        processed_payload["finmo_team_id"] = processed_payload.pop("finmoId")

    print(processed_payload)
    
    return {
        "url": args['org_url'],
        "headers": {'Authorization': token},
        "payload": {
            "wfe_app_id": "infin8v2",
            "entity_id": "organization",
            "api_request": json.dumps(processed_payload)
        }
    }

def create_user_group_payload(data, token):
    """Creates payload, URL and headers for user group entity"""
    mapping_response = get_mapping_response("organization", f"success#{data.get('organization_id')}")
    organization_id = mapping_response.get("data", {}).get("v2_value", "")
    if not organization_id:
        return get_failed_msg("organization", data.get("organization_id"))
    user_ids = convert_to_string_array(data.get("user_ids")) if "user_ids" in data else None

    user_group_payload = {
        "organization_id": organization_id,
        "group_name": data.get("group_name"),
        "user_ids": user_ids
    }
    print(user_group_payload)
    
    return {
        "url": args['org_url'],
        "headers": {'Authorization': token},
        "payload": {
            "wfe_app_id": "infin8v2",
            "entity_id": "infin8_users_group",
            "api_request": json.dumps(user_group_payload)
        }
    }

def create_account_payload(data, token):
    """Creates payload, URL and headers for account entity"""
    mapping_response = get_mapping_response("organization", f"success#{data.get('organization_id')}")
    organization_id = mapping_response.get("data", {}).get("v2_value", "")
    if not organization_id:
        return get_failed_msg("organization", data.get("organization_id"))

    mapping_response = get_mapping_response("user_group", f"success#{data.get('user_group_id')}")
    # Safely get the "v2_value" from the response
    user_group_data = mapping_response.get("data")
    user_group_id = user_group_data.get("v2_value") if user_group_data else None

    # if not user_group_id:
    #     return get_failed_msg("user", data.get("user_group_id"))

    commission = data.get("commission")

    # Handle the case when 'commission' is None
    if commission is None:
        commission = 0  # or any other default value you'd prefer

    # Convert the commission to an integer if it's not None
    commission = int(commission)
    

    account_payload = {
        "organization_id": organization_id,
        "account_name": data.get("account_name"),
        "account_type": data.get("account_type"),
        "description": data.get("description"),
        "campaign_name": data.get("campaign_name"),
        "commission": commission,
        "is_archived": convert_to_boolean("false"),
        "user_group_id": user_group_id
    }
    print(account_payload)

    return {
        "url": args['org_url'],
        "headers": {'Authorization': token},
        "payload": {
            "wfe_app_id": "infin8v2",
            "entity_id": "account",
            "api_request": json.dumps(account_payload)
        }
    }

def create_user_payload(data, token):
    """Creates payload, URL and headers for user entity"""
    processed_payload = filter_payload_keys(data, "pk", "sk", "id")
    mapping_response = get_mapping_response("organization", f"success#{data.get('organization_id')}")
    organization_id = mapping_response.get("data", {}).get("v2_value", "")
    if not organization_id:
        return get_failed_msg("organization", data.get("organization_id"))

    if "organization_id" in processed_payload:
        processed_payload["organization_id"] = organization_id
    if "team_lead_user_id" in processed_payload:
        if isinstance(processed_payload["team_lead_user_id"], str):
            mapping_response = get_mapping_response("user", f"success#{processed_payload['team_lead_user_id']}")
            team_lead_user_id = mapping_response.get("data", {}).get("v2_value", "")
            if not team_lead_user_id:
                return get_failed_msg("user", data.get("team_lead_user_id"))
            processed_payload["team_lead_user_id"] = team_lead_user_id
            
    if "date_of_birth" in processed_payload:
        processed_payload["date_of_birth"] = convert_to_timestamp(processed_payload["date_of_birth"])
    if "date_of_joining" in processed_payload:
        processed_payload["date_of_joining"] = convert_to_timestamp(processed_payload["date_of_joining"])
    if "agent_title" in processed_payload:
        processed_payload["agent_title"] = str(processed_payload["agent_title"])
    
    
    processed_payload = json.loads(json.dumps(processed_payload, cls=DecimalEncoder))
    print(processed_payload)
    
    return {
        "url": args['user_url'],
        "headers": {'Authorization': token},
        "payload": {
            "user_id": "995de72c-3ea4-4cc8-bda8-d81077b2621a",
            "user_role": "IMA",
            "data": processed_payload
        }
    }

def create_lender_payload(data, token):    
    lender_basic_info = {
        "is_lender_type": not data.get("is_lender_offline"),
        "lender_name": data.get("lender_name"),
        "lender_type": data.get("lender_type"),
        "lender_buydown_ratio": int(data.get("lender_buydown_ratio")),
        "lender_description": data.get("lender_description"),
        "lender_phone": data.get("lender_phone"),
        "is_active": data.get("is_active"),
        "website": data.get("website"),
        "lender_spotlight_id": data.get("lender_spotlight_id")
    }
    processed_payload = {
        "lender_basic_info": lender_basic_info,
        "lender_address": data.get("lender_address", [])[0]
    }

    processed_payload = json.loads(json.dumps(processed_payload, cls=DecimalEncoder))

    return {
        "url": args['lender_url'],
        "headers": {'Authorization': token},
        "payload": {
            "user_id": "d42f979-cd55-4fa9-a98e-1cca5a65a951",
            "user_role": "IMA",
            "data": processed_payload
        }
    }

def create_bdm_payload(bdm_list, token, response):
    bdm_payload_list = []
    for bdm_info in bdm_list:
        bdm_payload = {
            "lender_id": response.get("response", {}).get("data", {}).get("loan_lender_application_id"),
            "email_address": bdm_info.get("email_address"),
            "name": bdm_info.get("name"),
            "office_number": bdm_info.get("office_number"),
            "province": bdm_info.get("province"),
            "title": bdm_info.get("title")
        }
        bdm_payload_list.append(bdm_payload)

    payload = {
        "record_create_list": bdm_payload_list
    }

    return {
        "url": args['bdm_url'],
        "headers": {'Authorization': token},
        "payload": {
            "wfe_app_id": "infin8v2",
            "entity_id": "",
            "api_request": json.dumps(payload)
        }
    }

def create_deal_payload(data, token):
    processed_payload = filter_payload_keys(data, "pk", "sk", "id")
    processed_payload = json.loads(json.dumps(processed_payload, cls=DecimalEncoder))
    return {
        "url": f"{args['mock_url']}/fees",
        "headers": {'Authorization': token, 'x-mock-response-code': "201"},
        "payload": processed_payload
    }

def create_fms_payload(data, token):
    fms_payload = {
        "deal_id": data.get("deal_id"),
        "fms": data.get("fms")
    }
    return {
        "url": args['org_url'],
        "headers": {'Authorization': token, 'x-mock-response-code': "201"},
        "payload": fms_payload
    }

def create_loan_document_map_payload(data, token, fms_doc_ids, response):
    loan_document_map_payload = {
        "deal_id": data.get("deal_id"),
        "loan_document_map": data.get("loan_document_map")
    }
    loan_document_map_payload = json.loads(json.dumps(loan_document_map_payload, cls=DecimalEncoder))
    return {
        "url": f"{args['mock_url']}/documents",
        "headers": {'Authorization': token, 'x-mock-response-code': "201"},
        "payload": loan_document_map_payload
    }

def create_document_payload(data, token):
    document_payload = {
        "name": data.get("document_name"),
        "type": data.get("document_format"),
        "size": 1024,
        "storage_path": f"{BASE_S3_PATH}/{data.get('document_path')}"
    }
    document_payload = json.loads(json.dumps(document_payload, cls=DecimalEncoder))
    return {
        "url": f"{args['mock_url']}/documents",
        "headers": {'Authorization': token, 'x-mock-response-code': "201"},
        "payload": document_payload
    }

def create_fees_payload(fees_list, token, response):
    fees_payload_list = []
    for fee_info in fees_list:
        fee_payload = {
            "fee_id": fee_info.get("fee_id"),
            "amount": fee_info.get("amount"),
            "description": fee_info.get("description")
        }
        fees_payload_list.append(fee_payload)

    payload = {
        "record_create_list": fees_payload_list
    }

    return {
        "url": args['fees_url'],
        "headers": {'Authorization': token},
        "payload": {
            "wfe_app_id": "infin8v2",
            "entity_id": "",
            "api_request": json.dumps(payload)
        }
    }

def create_participants_payload(participants_list, token, response):
    participants_payload_list = []
    for participant_info in participants_list:
        participant_payload = {
            "participant_id": participant_info.get("participant_id"),
            "name": participant_info.get("name"),
            "role": participant_info.get("role")
        }
        participants_payload_list.append(participant_payload)

    payload = {
        "record_create_list": participants_payload_list
    }

    return {
        "url": args['participants_url'],
        "headers": {'Authorization': token},
        "payload": {
            "wfe_app_id": "infin8v2",
            "entity_id": "",
            "api_request": json.dumps(payload)
        }
    }

def create_sign_submission_status_payload(status_list, token, response):
    status_payload_list = []
    for status_info in status_list:
        status_payload = {
            "submission_id": status_info.get("submission_id"),
            "status": status_info.get("status"),
            "timestamp": status_info.get("timestamp")
        }
        status_payload_list.append(status_payload)

    payload = {
        "record_create_list": status_payload_list
    }

    return {
        "url": args['sign_submission_status_url'],
        "headers": {'Authorization': token},
        "payload": {
            "wfe_app_id": "infin8v2",
            "entity_id": "",
            "api_request": json.dumps(payload)
        }
    }

def create_credit_consent_payload(consent_list, token, response):
    consent_payload_list = []
    for consent_info in consent_list:
        consent_payload = {
            "consent_id": consent_info.get("consent_id"),
            "consent_status": consent_info.get("consent_status"),
            "timestamp": consent_info.get("timestamp")
        }
        consent_payload_list.append(consent_payload)

    payload = {
        "record_create_list": consent_payload_list
    }

    return {
        "url": args['credit_consent_url'],
        "headers": {'Authorization': token},
        "payload": {
            "wfe_app_id": "infin8v2",
            "entity_id": "",
            "api_request": json.dumps(payload)
        }
    }

def create_credit_pull_payload(pull_list, token, response):
    pull_payload_list = []
    for pull_info in pull_list:
        pull_payload = {
            "pull_id": pull_info.get("pull_id"),
            "credit_score": pull_info.get("credit_score"),
            "timestamp": pull_info.get("timestamp")
        }
        pull_payload_list.append(pull_payload)

    payload = {
        "record_create_list": pull_payload_list
    }

    return {
        "url": args['credit_pull_url'],
        "headers": {'Authorization': token},
        "payload": {
            "wfe_app_id": "infin8v2",
            "entity_id": "",
            "api_request": json.dumps(payload)
        }
    }

def create_reference_mapping_payload(mapping_list, token, response):
    mapping_payload_list = []
    for mapping_info in mapping_list:
        mapping_payload = {
            "mapping_id": mapping_info.get("mapping_id"),
            "source": mapping_info.get("source"),
            "target": mapping_info.get("target")
        }
        mapping_payload_list.append(mapping_payload)

    payload = {
        "record_create_list": mapping_payload_list
    }

    return {
        "url": args['reference_mapping_url'],
        "headers": {'Authorization': token},
        "payload": {
            "wfe_app_id": "infin8v2",
            "entity_id": "",
            "api_request": json.dumps(payload)
        }
    }


def create_deal_note_payload(data, token):
    processed_payload = filter_payload_keys(data, "pk", "sk", "id")
    processed_payload = json.loads(json.dumps(processed_payload, cls=DecimalEncoder))
    return {
        "url": f"{args['mock_url']}/deal_notes",
        "headers": {'Authorization': token, 'x-mock-response-code': "201"},
        "payload": processed_payload
    }

def create_deal_task_payload(data, token):
    processed_payload = filter_payload_keys(data, "pk", "sk", "id")
    processed_payload = json.loads(json.dumps(processed_payload, cls=DecimalEncoder))
    return {
        "url": f"{args['mock_url']}/deal_tasks",
        "headers": {'Authorization': token, 'x-mock-response-code': "201"},
        "payload": processed_payload
    }

# ---- ceate_contact_payload-------------------------------------------------------
def create_contact_payload(data, token):
    """Creates payload, URL and headers for user entity"""
    # Remove keys is required - 
    processed_payload = filter_payload_keys(data, "pk", "sk", "id")
    # # update v2 organization id
    # mapping_response = get_mapping_response("organization", f"success#{data.get('org_id')}")
    # organization_id = mapping_response.get("data", {}).get("v2_value", "")
    # if not organization_id:
    #     return get_failed_msg("organization", data.get("organization_id"))
    # # update v2 user id
    # mapping_response = get_mapping_response("user", f"success#{data.get('contact_owner_id')}")
    # user_id = mapping_response.get("data", {}).get("v2_value", "")
    # if not user_id:
    #     return get_failed_msg("user", data.get("contact_owner_id"))
    # get mapping response for email ID
    print(f"data value{data}")
    mapping_response = get_mapping_response("contact", f"success#{data.get('email_id')}")

    # processed_payload["org_id"] = organization_id
    # processed_payload["contact_owner_id"] = user_id

    # if contact already exists, merge the contact details
    if mapping_response.get("data"):
        mapping_response = json.loads(mapping_response.get("data", {}).get("payload"))
        processed_payload = get_merged_contact_payload(processed_payload, mapping_response)

    
    print(processed_payload)
    processed_payload = json.loads(json.dumps(processed_payload, cls=DecimalEncoder))
    print("after processed json:+==========")
    print(processed_payload)
    
    return {
        "url": args['contact_url'],
        "headers": {'Authorization': token, 'x-mock-response-code': "201"},
        "payload": processed_payload
    }

# ---- ceate_contact_note_payload-------------------------------------------------------
def create_contact_note_payload(data, token):
    """Creates payload, URL and headers for user entity"""
    # Remove keys is required - 
    processed_payload = filter_payload_keys(data, "pk", "sk", "id")
    # # update v2 organization id
    # mapping_response = get_mapping_response("organization", f"success#{data.get('organization_id')}")
    # organization_id = mapping_response.get("data", {}).get("v2_value", "")
    # if not organization_id:
    #     return get_failed_msg("organization", data.get("organization_id"))
    # # update v2 contact
    # mapping_response = get_mapping_response("contact", f"success#{data.get('email_id')}")
    # contact_id = mapping_response.get("data", {}).get("v2_value", "")
    # if not contact_id:
    #     return get_failed_msg("contact", data.get("email_id"))
    # processed_payload["organization_id"] = organization_id
    # processed_payload["contact_id"] = contact_id

    processed_payload = json.loads(json.dumps(processed_payload, cls=DecimalEncoder))
    print(processed_payload)
    return {
        "url": args['contact_note_url'],
        "headers": {'Authorization': token, 'x-mock-response-code': "201"},
        "payload": processed_payload
    }

# ---- ceate_contact_note_payload-------------------------------------------------------
def create_contact_task_payload(data, token):
    """Creates payload, URL and headers for user entity"""
    # Remove keys is required - 
    processed_payload = filter_payload_keys(data, "pk", "sk", "id")
    # # update v2 organization id
    # mapping_response = get_mapping_response("organization", f"success#{data.get('organization_id')}")
    # organization_id = mapping_response.get("data", {}).get("v2_value", "")
    # if not organization_id:
    #     return get_failed_msg("organization", data.get("organization_id"))
    # # update v2 contact
    # mapping_response = get_mapping_response("contact", f"success#{data.get('email_id')}")
    # contact_id = mapping_response.get("data", {}).get("v2_value", "")
    # if not contact_id:
    #     return get_failed_msg("contact", data.get("email_id"))
    # processed_payload["organization_id"] = organization_id
    # processed_payload["contact_id"] = contact_id
    
    processed_payload = json.loads(json.dumps(processed_payload, cls=DecimalEncoder))
    print(processed_payload)
    return {
        "url": args['contact_task_url'],
        "headers": {'Authorization': token, 'x-mock-response-code': "201"},
        "payload": processed_payload
    }



# ----get-mapping-response-------------------------------------------------------

def get_mapping_response(pk: str, sk: str) -> dict:
    """
    Retrieves the full mapping record from DynamoDB using pk and sk.

    """
    if not pk or not sk:
        return {
            'success': False,
            'data': None,
            'error': "Missing required parameters: pk or sk"
        }


    try:
        response = table.get_item(Key={
            "pk": pk,
            "sk": sk
        })

        if "Item" not in response:
            return {
                'success': False,
                'data': None,
                'error': f"No mapping found for pk={pk}, sk={sk}"
            }

        return {
            'success': True,
            'data': response["Item"],
            'error': None
        }

    except Exception as e:
        print(f"Error retrieving mapping: {str(e)}")
        return {
            'success': False,
            'data': None,
            'error': str(e)
        }


# ----create-dynamo-record-------------------------------------------------------

def create_dynamo_record(table, pk: str, sk: str, v1_value: str, result: str, payload: str, v2_value: str = None, error_msg: str = None, response: str = None):
    try:

        item = {
            'pk': pk,
            'sk': sk,
            'v1_value': v1_value,
            'result': result,
            'payload': payload
        }
        if v2_value:
            item['v2_value'] = v2_value
        if error_msg:
            item['errorMsg'] = error_msg
        if response:
            item['response'] = response
            
        table.put_item(Item=item)
        print(f"Successfully created {result} record for pk: {pk},sk: {sk}")
        
    except (BotoCoreError, ClientError) as e:
        print(f"Error creating DynamoDB record: {e}")


# ----filter-payload-keys for payload creation-------------------------------------------------------

def filter_payload_keys(payload: dict, *keys_to_remove: str) -> dict:
    try:
        if not isinstance(payload, dict):

            print(f"Invalid payload type: {type(payload)}")
            return {}

        remove_keys = set(keys_to_remove)
        
        return {
            k: v for k, v in payload.items() 
            if k not in remove_keys
        }
        
    except Exception as e:
        print(f"Error cleaning payload: {str(e)}")
        return {}




# ----process-api-response-------------------------------------------------------

def process_response(response_json):
    """

    Processes the API response and standardizes the result format
    """
    try:
        if 'api_response' in response_json:
            try:
                parsed_response = json.loads(response_json['api_response'])
                return {
                    'isSuccess': parsed_response.get('statusCode') == "200",
                    'response' if parsed_response.get('statusCode') == "200" else 'error': parsed_response
                }
            except json.JSONDecodeError:
                return {'isSuccess': False, 'error': 'Failed to parse api response'}
        
        if 'error' in response_json:
            return {'isSuccess': False, 'error': response_json}
        
        if 'status' in response_json:
            status = response_json['status']
            if isinstance(status, bool):
                return {
                    'isSuccess': status,
                    'response' if status else 'error': response_json
                }
            status = str(status).upper()
            return {
            'isSuccess': status in ('OK', 'SUCCESS'),
            'response' if status in ('OK', 'SUCCESS') else 'error': response_json
            }
        
        return {'isSuccess': True, 'response': response_json}
        
    except Exception as e:
        return {'isSuccess': False, 'error': str(e)}


def process_bulk_response(response_json):
    """
    Processes the bulk API response and standardizes the result format.
    """
    try:
        if 'result' in response_json:
            results = response_json['result']
            processed_results = []

            for result in results:
                status_code = result.get('statusCode')
                is_success = status_code == 200
                processed_result = {
                    'isSuccess': is_success,
                    'response' if is_success else 'error': result
                }
                processed_results.append(processed_result)

            return {
                'isSuccess': response_json.get('header_status_code') == 200,
                'results': processed_results,
                'total_record_created_success_count': response_json.get('total_record_created_success_count', 0),
                'total_record_created_failure_count': response_json.get('total_record_created_failure_count', 0),
                'total_records_count': response_json.get('total_records_count', 0)
            }
        
        return {'isSuccess': False, 'error': 'Invalid response format'}
        
    except Exception as e:
        return {'isSuccess': False, 'error': str(e)}
 
# ----get-v2-id-for-mapping------------------------------------------------------

def get_v2_id(api_response: dict | None) -> str | None:
    if not isinstance(api_response, dict):
        print(f"Invalid API response type: {type(api_response)}")

        return None
    
    try:
        response_data = api_response.get('result') or api_response.get('data') or {}

        if not isinstance(response_data, dict):
            print(f"Invalid result type: {type(response_data)}")
            return None

        application_id = response_data.get('application_id') or response_data.get('loan_lender_application_id')
        if not application_id:
            print(f"Response data: {response_data}")
            return None

        return str(application_id)
        
    except Exception as e:
        print(f"Error extracting application_id: {str(e)}")
        print(f"Full response: {api_response}")
        return None



# ----helper-functions-------------------------------------------------------

class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder for Decimal types"""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_auth_token(auth_url, client_id, username, password, client_secret, max_retries=3, retry_delay=1):
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'client_id': client_id,
        'username': username,
        'password': password,
        'grant_type': 'password',
        'client_secret': client_secret
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.post(
                auth_url, 
                headers=headers, 
                data=urlencode(data),
                timeout=10  # Add timeout to prevent hanging
            )
            response.raise_for_status()
            
            token = response.json().get('access_token')
            if not token:
                raise ValueError("No access token in response")
                
            print(f"Successfully obtained auth token on attempt {attempt + 1}")
            return token
            
        except requests.exceptions.Timeout:
            print(f"Timeout error on attempt {attempt + 1}")
        except requests.exceptions.ConnectionError:
            print(f"Connection error on attempt {attempt + 1}")
        except requests.exceptions.RequestException as e:
            print(f"Error obtaining auth token on attempt {attempt + 1}: {str(e)}")
        except (ValueError, KeyError, json.JSONDecodeError) as e:
            print(f"Invalid response format on attempt {attempt + 1}: {str(e)}")
            
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
            
    print("Failed to obtain auth token after all retries")
    return None

def convert_to_timestamp(date_str):
    if not date_str:
        return None
    try:
        return int(datetime.fromisoformat(date_str.rstrip('Z') + '+00:00').timestamp())
    except ValueError:
        print(f"Invalid date format: {date_str}")
        return None

def convert_to_boolean(value: str) -> bool | None:
    if not value or value.strip() == "":  # Handle None or empty string
        return None
    value = str(value).strip().lower()
    return value in ('true', '1', 'yes', 'y', 't')

def convert_to_string_array(value_str: str | None) -> list[str]:
    """
    Convert a string representation of an array to a list of strings.
    Args:
        value_str: String representation of an array (e.g., "[1444,1548,1494]")
    Returns:
        List of strings, or empty list if input is invalid
    """
    if not value_str or not isinstance(value_str, str):
        return []
    
    # One-line list comprehension with direct string operations
    return [x for x in value_str.strip('[]').replace(' ', '').split(',') if x]


def get_failed_msg(entity_name, entity_v1_id):
    return {
            "result": "failed",
            "request_attribute": None,
            "msg": f"v2_not_found:{entity_name}:{entity_v1_id}"
        }


# ----get-merged-contact-payload-------------------------------------------------------
def get_merged_contact_payload(processed_payload, mapping_response):
    """Merges the contact details based on priority roles."""
    print(f"******existing input{mapping_response}")
    print(f"******current input{processed_payload}")
    priority = ["client", "businesspartner", "solicitor", "realtor", "appraiser"]
    
    # Get the role_based_attributes attribute from mapping response
    current_role_based_attributes = mapping_response.get("role_based_attributes", [])
    print(f"current_role_based_attributes{current_role_based_attributes}")

    # Find the highest available role based on priority
    highest_priority_role = None
    for role in priority:
        for item in current_role_based_attributes:
            print(f"item{item}")
            if item.get("role").lower() == role:
                highest_priority_role = role
                break
        if highest_priority_role:
            break
    
    # Get the current role from input payload
    current_role = processed_payload.get("role_based_attributes")[0].get("role")
    
    # If the current role has higher priority, replace fields
    if priority.index(current_role.lower()) < priority.index(highest_priority_role.lower()): 
        processed_payload.update({
            "id": processed_payload.get("id") or mapping_response.get("id"),
            "external_id": processed_payload.get("external_id") or mapping_response.get("external_id"),
            "org_id": processed_payload.get("org_id") or mapping_response.get("org_id"),
            "contact_owner_id": processed_payload.get("contact_owner_id") or mapping_response.get("contact_owner_id"),
            "first_name": processed_payload.get("first_name") or mapping_response.get("first_name"),
            "last_name": processed_payload.get("last_name") or mapping_response.get("last_name"),
            "middle_name": processed_payload.get("middle_name") or mapping_response.get("middle_name"),
            "salutation": processed_payload.get("salutation") or mapping_response.get("salutation"),
            "suffix": processed_payload.get("suffix") or mapping_response.get("suffix"),
            "date_of_birth": processed_payload.get("date_of_birth") or mapping_response.get("date_of_birth"),
            "gender": processed_payload.get("gender") or mapping_response.get("gender"),
            "marital_status": processed_payload.get("marital_status") or mapping_response.get("marital_status"),
            "email_id": processed_payload.get("email_id") or mapping_response.get("email_id"),
            "language_preference": processed_payload.get("language_preference") or mapping_response.get("language_preference")
        })
    
    # Append the role_based_attributes object with response's role_based_attributes array
    processed_payload["role_based_attributes"] = processed_payload.get("role_based_attributes", []) + current_role_based_attributes
    
    return processed_payload

# ----handle-failed-payload-creation-------------------------------------------------------

def handle_failed_payload_creation(record, payload, pk = None, sk = None):
    if "result" in payload and payload["result"] == "failed":
        print("Failed to create payload, skipping...")
        create_dynamo_record(
            table=table,
            pk=pk or record.get("pk"),
            sk=f"failed#{sk or record.get('sk')}",
            v1_value=sk or record.get("sk"),
            result='failed',
            payload=record,
            error_msg=payload["msg"]
        )
        return True
    return False


# ----process-entity-------------------------------------------------------

def processEntity(item, token):
    entity_map = {
        "organization": process_organization,
        "user_group": process_user_group,
        "account": process_account,
        "user": process_user,
        "contact": process_contact,
        "contact_note": process_contact_note,
        "contact_task": process_contact_task,
        "lender": process_lender,
        "deal": process_deal,
        "deal_note": process_deal_note,
        "deal_task": process_deal_task
    }
    pk = item.get("pk")
    process_function = entity_map.get(pk)
    
    if process_function:
        return process_function(item, token)
        
    print(f"Warning: Unknown entity type with pk={pk}")
    return None

# ----orchestration functions-------------------------------------------------------

def process_organization(record, token):
    org_payload = create_organization_payload(record, token)
    result = handle_failed_payload_creation(record, org_payload)
    if result: return

    response, new_token, sent_payload = executeApi(org_payload, token)
    token = new_token if new_token else token
    updateMapping(response, record, sent_payload)


def process_user_group(record, token):
    user_group_payload = create_user_group_payload(record, token)
    result = handle_failed_payload_creation(record, user_group_payload)
    if result: return

    response, new_token, sent_payload = executeApi(user_group_payload, token)
    token = new_token if new_token else token
    updateMapping(response, record, sent_payload)

def process_account(record, token):
    account_payload = create_account_payload(record, token)
    result = handle_failed_payload_creation(record, account_payload)
    if result: return

    response, new_token, sent_payload = executeApi(account_payload, token)
    token = new_token if new_token else token
    updateMapping(response, record, sent_payload)

def process_user(record, token):
    user_payload = create_user_payload(record, token)
    result = handle_failed_payload_creation(record, user_payload)
    if result: return

    response, new_token, sent_payload = executeApi(user_payload, token)
    token = new_token if new_token else token
    updateMapping(response, record, sent_payload)

def process_lender(record, token):
    # first payload
    lender_payload = create_lender_payload(record, token)
    result = handle_failed_payload_creation(record, lender_payload)
    if result: return

    response, new_token, sent_payload = executeApi(lender_payload, token)
    token = new_token if new_token else token
    updateMapping(response, record, sent_payload)

    # second payload
    bdm_list = record.get("lender_business_development_manager", [])
    bdm_payload = create_bdm_payload(bdm_list, token, response)
    pk = record.get('pk')
    sk_id = f"{record.get('sk')}#bdm"
    result = handle_failed_payload_creation(record, bdm_payload, pk, sk_id)
    if result: return

    response, new_token, sent_payload = exectueBulkAPI(bdm_payload, token)
    token = new_token if new_token else token
    updateMappingForBulkResponses(response, record, sent_payload, sk_id)

def process_deal(record, token):
    deal_payload = create_deal_payload(record, token)
    result = handle_failed_payload_creation(record, deal_payload)
    if result: return

    dealresponse, new_token, sent_payload = executeApi(deal_payload, token)
    token = new_token if new_token else token
    updateMapping(dealresponse, record, sent_payload)

    loan_document_map_list = record.get("documents", [])
    for loan_document_map in loan_document_map_list:
        docs = loan_document_map.get("docs", [])
        fms_doc_ids = []
        for doc in docs:
            doc_payload = create_document_payload(doc, token)
            pk = record.get('pk')
            sk_id = f"{record.get('sk')}#document#docs#{uuid.uuid4()}"
            result = handle_failed_payload_creation(record, doc_payload, pk, sk_id)
            if result: return

            response, new_token, sent_payload = executeApi(doc_payload, token)
            fms_doc_ids.append(response.get("id"))
            token = new_token if new_token else token
            updateMapping(response, record, sent_payload, sk_id)
            
        loan_document_map_payload = create_loan_document_map_payload(loan_document_map, token, fms_doc_ids, dealresponse)
        pk = record.get('pk')
        sk_id = f"{record.get('sk')}#document#{uuid.uuid4()}"
        result = handle_failed_payload_creation(record, loan_document_map_payload, pk, sk_id)
        if result: return

        response, new_token, sent_payload = executeApi(loan_document_map_payload, token)
        token = new_token if new_token else token
        updateMapping(response, record, sent_payload, sk_id)


    fees_list = record.get("fees", [])
    fees_payload = create_fees_payload(fees_list, token, dealresponse)
    pk = record.get('pk')
    sk_id = f"{record.get('sk')}#fees"
    result = handle_failed_payload_creation(record, fees_payload, pk, sk_id)
    if result: return

    response, new_token, sent_payload = exectueBulkAPI(fees_payload, token)
    token = new_token if new_token else token
    updateMappingForBulkResponses(response, record, sent_payload, sk_id)

    participants_list = record.get("participants", [])
    participants_payload = create_participants_payload(participants_list, token, dealresponse)
    pk = record.get('pk')
    sk_id = f"{record.get('sk')}#participants"
    result = handle_failed_payload_creation(record, participants_payload, pk, sk_id)
    if result: return

    response, new_token, sent_payload = exectueBulkAPI(participants_payload, token)
    token = new_token if new_token else token   
    updateMappingForBulkResponses(response, record, sent_payload, sk_id)
    
    sign_submission_list = record.get("sign_submission_status", [])
    sign_submission_payload = create_sign_submission_status_payload(sign_submission_list, token, dealresponse)
    pk = record.get('pk')
    sk_id = f"{record.get('sk')}#sign_submission_status"
    result = handle_failed_payload_creation(record, sign_submission_payload, pk, sk_id)
    if result: return

    response, new_token, sent_payload = exectueBulkAPI(sign_submission_payload, token)
    token = new_token if new_token else token
    updateMappingForBulkResponses(response, record, sent_payload, sk_id)
    
    
    credit_consent_list = record.get("credit_consent", [])
    credit_consent_payload = create_credit_consent_payload(credit_consent_list, token, dealresponse)
    pk = record.get('pk')
    sk_id = f"{record.get('sk')}#credit_consent"
    result = handle_failed_payload_creation(record, credit_consent_payload, pk, sk_id)
    if result: return

    response, new_token, sent_payload = exectueBulkAPI(credit_consent_payload, token)
    token = new_token if new_token else token
    updateMappingForBulkResponses(response, record, sent_payload, sk_id)


    credit_pull_list = record.get("credit_pull", [])
    credit_pull_payload = create_credit_pull_payload(credit_pull_list, token, dealresponse)
    pk = record.get('pk')
    sk_id = f"{record.get('sk')}#credit_pull"
    result = handle_failed_payload_creation(record, credit_pull_payload, pk, sk_id)
    if result: return

    response, new_token, sent_payload = exectueBulkAPI(credit_pull_payload, token)
    token = new_token if new_token else token
    updateMappingForBulkResponses(response, record, sent_payload, sk_id)
    

    reference_mapping_list = record.get("refrence_mapping", [])
    reference_mapping_payload = create_reference_mapping_payload(reference_mapping_list, token, dealresponse)
    pk = record.get('pk')
    sk_id = f"{record.get('sk')}#refrence_mapping"
    result = handle_failed_payload_creation(record, reference_mapping_payload, pk, sk_id)
    if result: return

    response, new_token, sent_payload = exectueBulkAPI(reference_mapping_payload, token)
    token = new_token if new_token else token
    updateMappingForBulkResponses(response, record, sent_payload, sk_id)
    

def process_deal_note(record, token):
    deal_note_payload = create_deal_note_payload(record, token)
    result = handle_failed_payload_creation(record, deal_note_payload)
    if result: return

    response, new_token, sent_payload = executeApi(deal_note_payload, token)
    token = new_token if new_token else token
    updateMapping(response, record, sent_payload)

def process_deal_task(record, token):
    deal_task_payload = create_deal_task_payload(record, token)
    result = handle_failed_payload_creation(record, deal_task_payload)
    if result: return

    response, new_token, sent_payload = executeApi(deal_task_payload, token)
    token = new_token if new_token else token
    updateMapping(response, record, sent_payload)

def process_contact(record, token):
    contact_payload = create_contact_payload(record, token)
    result = handle_failed_payload_creation(record, contact_payload)
    if result: return
    response, new_token, sent_payload = executeApi(contact_payload, token)
    token = new_token if new_token else token
    updateMapping(response, record, sent_payload)

def process_contact_note(record, token):
    contact_note_payload = create_contact_note_payload(record, token)
    result = handle_failed_payload_creation(record, contact_note_payload)
    if result: return
    
    response, new_token, sent_payload = executeApi(contact_note_payload, token)
    token = new_token if new_token else token
    updateMapping(response, record, sent_payload)

def process_contact_task(record, token):
    contact_task_payload = create_contact_task_payload(record, token)
    result = handle_failed_payload_creation(record, contact_task_payload)
    if result: return
    
    response, new_token, sent_payload = executeApi(contact_task_payload, token)
    token = new_token if new_token else token
    updateMapping(response, record, sent_payload)



# ----process-data main function-------------------------------------------------------

    
def main(glueContext, dfc) -> DynamicFrameCollection:
    if args['OPERATION'] != "process":
        dynamic_frame = dfc.select(list(dfc.keys())[0])
        df = dynamic_frame.toDF()
        df.show()
        return dfc
    try:
        dynamic_frame = dfc.select(list(dfc.keys())[0])
        df = dynamic_frame.toDF()
        new_image_df = df.select(col("dynamodb.NewImage.*")).drop("SizeBytes")
        
        for row in new_image_df.toJSON().collect():
            try:
                item = processRecord(row)
                if not item:
                    print("Failed to process record, skipping...")
                    continue
                processEntity(item, token)
                
            except Exception as e:
                print(f"Error processing record: {str(e)}")
                continue

    except Exception as e:
        print(f"Error in processData: {str(e)}")
    
    return dfc


args = getResolvedOptions(sys.argv, ['JOB_NAME', 'OPERATION', 'org_url', 'user_url', 'lender_url', 'bdm_url', 'auth_url', 'client_id', 'username', 'password', 'client_secret', 'contact_url', 'contact_task_url','contact_note_url', 'mock_url'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Obtain the initial auth token outside the main function
token = get_auth_token(args['auth_url'], args['client_id'], args['username'], args['password'], args['client_secret'])
if not token:
    raise ValueError("Failed to obtain initial auth token")

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1735892796137 = glueContext.create_data_frame.from_options(connection_type="kinesis",connection_options={"typeOfData": "kinesis", "streamARN": "arn:aws:kinesis:us-east-1:577638400901:stream/data-migration-processing", "classification": "json", "startingPosition": "earliest", "inferSchema": "true"}, transformation_ctx="dataframe_AmazonKinesis_node1735892796137")

def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        AmazonKinesis_node1735892796137 = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        # Script generated for node Custom Transform
        CustomTransform_node1735892814368 = main(glueContext, DynamicFrameCollection({"AmazonKinesis_node1735892796137": AmazonKinesis_node1735892796137}, glueContext))

glueContext.forEachBatch(frame = dataframe_AmazonKinesis_node1735892796137, batch_function = processBatch, options = {"windowSize": "100 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"})
job.commit()
