import logging
from typing import Any, Optional
import os
import json
from datetime import datetime, timedelta
from dateutil.tz import tzlocal
from hubspot import HubSpot
from hubspot.crm.contacts.exceptions import ApiException

logger = logging.getLogger('mcp_hubspot_client')

def convert_datetime_fields(obj: Any) -> Any:
    """Convert any datetime or tzlocal objects to string in the given object"""
    if isinstance(obj, dict):
        return {k: convert_datetime_fields(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_fields(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, tzlocal):
        # Get the current timezone offset
        offset = datetime.now(tzlocal()).strftime('%z')
        return f"UTC{offset[:3]}:{offset[3:]}"  # Format like "UTC+08:00" or "UTC-05:00"
    return obj

class HubSpotClient:
    def __init__(self, access_token: Optional[str] = None):
        access_token = access_token or os.getenv("HUBSPOT_ACCESS_TOKEN")
        logger.debug(f"Using access token: {'[MASKED]' if access_token else 'None'}")
        if not access_token:
            raise ValueError("HUBSPOT_ACCESS_TOKEN environment variable is required")
        
        self.client = HubSpot(access_token=access_token)

    def get_recent_companies(self, limit: int = 10) -> str:
        """Get most recently active companies from HubSpot
        
        Args:
            limit: Maximum number of companies to return (default: 10)
        """
        try:
            from hubspot.crm.companies import PublicObjectSearchRequest
            
            # Create search request with sort by lastmodifieddate
            search_request = PublicObjectSearchRequest(
                sorts=[{
                    "propertyName": "lastmodifieddate",
                    "direction": "DESCENDING"
                }],
                limit=limit,
                properties=["name", "domain", "website", "phone", "industry", "hs_lastmodifieddate"]
            )
            
            # Execute the search
            search_response = self.client.crm.companies.search_api.do_search(
                public_object_search_request=search_request
            )
            
            # Convert the response to a dictionary
            companies_dict = [company.to_dict() for company in search_response.results]
            converted_companies = convert_datetime_fields(companies_dict)
            return json.dumps(converted_companies)
            
        except ApiException as e:
            logger.error(f"API Exception: {str(e)}")
            return json.dumps({"error": str(e)})
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            return json.dumps({"error": str(e)})

    def get_recent_contacts(self, limit: int = 10) -> str:
        """Get most recently active contacts from HubSpot
        
        Args:
            limit: Maximum number of contacts to return (default: 10)
        """
        try:
            from hubspot.crm.contacts import PublicObjectSearchRequest
            
            # Create search request with sort by lastmodifieddate
            search_request = PublicObjectSearchRequest(
                sorts=[{
                    "propertyName": "lastmodifieddate",
                    "direction": "DESCENDING"
                }],
                limit=limit,
                properties=["firstname", "lastname", "email", "phone", "company", "hs_lastmodifieddate", "lastmodifieddate"]
            )
            
            # Execute the search
            search_response = self.client.crm.contacts.search_api.do_search(
                public_object_search_request=search_request
            )
            
            # Convert the response to a dictionary
            contacts_dict = [contact.to_dict() for contact in search_response.results]
            converted_contacts = convert_datetime_fields(contacts_dict)
            return json.dumps(converted_contacts)
            
        except ApiException as e:
            logger.error(f"API Exception: {str(e)}")
            return json.dumps({"error": str(e)})
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            return json.dumps({"error": str(e)})

    def get_company_activity(self, company_id: str) -> str:
        """Get activity history for a specific company"""
        try:
            # Step 1: Get all engagement IDs associated with the company using CRM Associations v4 API
            associated_engagements = self.client.crm.associations.v4.basic_api.get_page(
                object_type="companies",
                object_id=company_id,
                to_object_type="engagements",
                limit=500
            )
            
            # Extract engagement IDs from the associations response
            engagement_ids = []
            if hasattr(associated_engagements, 'results'):
                for result in associated_engagements.results:
                    engagement_ids.append(result.to_object_id)

            # Step 2: Get detailed information for each engagement
            activities = []
            for engagement_id in engagement_ids:
                engagement_response = self.client.api_request({
                    "method": "GET",
                    "path": f"/engagements/v1/engagements/{engagement_id}"
                }).json()
                
                engagement_data = engagement_response.get('engagement', {})
                metadata = engagement_response.get('metadata', {})
                
                # Format the engagement
                formatted_engagement = {
                    "id": engagement_data.get("id"),
                    "type": engagement_data.get("type"),
                    "created_at": engagement_data.get("createdAt"),
                    "last_updated": engagement_data.get("lastUpdated"),
                    "created_by": engagement_data.get("createdBy"),
                    "modified_by": engagement_data.get("modifiedBy"),
                    "timestamp": engagement_data.get("timestamp"),
                    "associations": engagement_response.get("associations", {})
                }
                
                # Add type-specific metadata formatting
                if engagement_data.get("type") == "NOTE":
                    formatted_engagement["content"] = metadata.get("body", "")
                elif engagement_data.get("type") == "EMAIL":
                    formatted_engagement["content"] = {
                        "subject": metadata.get("subject", ""),
                        "from": {
                            "raw": metadata.get("from", {}).get("raw", ""),
                            "email": metadata.get("from", {}).get("email", ""),
                            "firstName": metadata.get("from", {}).get("firstName", ""),
                            "lastName": metadata.get("from", {}).get("lastName", "")
                        },
                        "to": [{
                            "raw": recipient.get("raw", ""),
                            "email": recipient.get("email", ""),
                            "firstName": recipient.get("firstName", ""),
                            "lastName": recipient.get("lastName", "")
                        } for recipient in metadata.get("to", [])],
                        "cc": [{
                            "raw": recipient.get("raw", ""),
                            "email": recipient.get("email", ""),
                            "firstName": recipient.get("firstName", ""),
                            "lastName": recipient.get("lastName", "")
                        } for recipient in metadata.get("cc", [])],
                        "bcc": [{
                            "raw": recipient.get("raw", ""),
                            "email": recipient.get("email", ""),
                            "firstName": recipient.get("firstName", ""),
                            "lastName": recipient.get("lastName", "")
                        } for recipient in metadata.get("bcc", [])],
                        "sender": {
                            "email": metadata.get("sender", {}).get("email", "")
                        },
                        "body": metadata.get("text", "") or metadata.get("html", "")
                    }
                elif engagement_data.get("type") == "TASK":
                    formatted_engagement["content"] = {
                        "subject": metadata.get("subject", ""),
                        "body": metadata.get("body", ""),
                        "status": metadata.get("status", ""),
                        "for_object_type": metadata.get("forObjectType", "")
                    }
                elif engagement_data.get("type") == "MEETING":
                    formatted_engagement["content"] = {
                        "title": metadata.get("title", ""),
                        "body": metadata.get("body", ""),
                        "start_time": metadata.get("startTime"),
                        "end_time": metadata.get("endTime"),
                        "internal_notes": metadata.get("internalMeetingNotes", "")
                    }
                elif engagement_data.get("type") == "CALL":
                    formatted_engagement["content"] = {
                        "body": metadata.get("body", ""),
                        "from_number": metadata.get("fromNumber", ""),
                        "to_number": metadata.get("toNumber", ""),
                        "duration_ms": metadata.get("durationMilliseconds"),
                        "status": metadata.get("status", ""),
                        "disposition": metadata.get("disposition", "")
                    }
                
                activities.append(formatted_engagement)

            # Convert any datetime fields and return
            converted_activities = convert_datetime_fields(activities)
            return json.dumps(converted_activities)
            
        except ApiException as e:
            logger.error(f"API Exception: {str(e)}")
            return json.dumps({"error": str(e)})
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            return json.dumps({"error": str(e)})

    def get_recent_engagements(self, days: int = 7, limit: int = 50) -> str:
        """Get recent engagements across all contacts/companies
        
        Args:
            days: Number of days to look back (default: 7)
            limit: Maximum number of engagements to return (default: 50)
        """
        try:
            # Calculate the date range (past N days)
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)
            
            # Format timestamps for API call
            start_timestamp = int(start_time.timestamp() * 1000)  # Convert to milliseconds
            end_timestamp = int(end_time.timestamp() * 1000)  # Convert to milliseconds
            
            # Get all recent engagements
            engagements_response = self.client.api_request({
                "method": "GET",
                "path": f"/engagements/v1/engagements/recent/modified",
                "params": {
                    "count": limit,
                    "since": start_timestamp,
                    "offset": 0
                }
            }).json()
            
            # Format the engagements similar to company_activity
            formatted_engagements = []
            
            for engagement in engagements_response.get('results', []):
                engagement_data = engagement.get('engagement', {})
                metadata = engagement.get('metadata', {})
                
                formatted_engagement = {
                    "id": engagement_data.get("id"),
                    "type": engagement_data.get("type"),
                    "created_at": engagement_data.get("createdAt"),
                    "last_updated": engagement_data.get("lastUpdated"),
                    "created_by": engagement_data.get("createdBy"),
                    "modified_by": engagement_data.get("modifiedBy"),
                    "timestamp": engagement_data.get("timestamp"),
                    "associations": engagement.get("associations", {})
                }
                
                # Add type-specific metadata formatting identical to company_activity
                if engagement_data.get("type") == "NOTE":
                    formatted_engagement["content"] = metadata.get("body", "")
                elif engagement_data.get("type") == "EMAIL":
                    formatted_engagement["content"] = {
                        "subject": metadata.get("subject", ""),
                        "from": {
                            "raw": metadata.get("from", {}).get("raw", ""),
                            "email": metadata.get("from", {}).get("email", ""),
                            "firstName": metadata.get("from", {}).get("firstName", ""),
                            "lastName": metadata.get("from", {}).get("lastName", "")
                        },
                        "to": [{
                            "raw": recipient.get("raw", ""),
                            "email": recipient.get("email", ""),
                            "firstName": recipient.get("firstName", ""),
                            "lastName": recipient.get("lastName", "")
                        } for recipient in metadata.get("to", [])],
                        "cc": [{
                            "raw": recipient.get("raw", ""),
                            "email": recipient.get("email", ""),
                            "firstName": recipient.get("firstName", ""),
                            "lastName": recipient.get("lastName", "")
                        } for recipient in metadata.get("cc", [])],
                        "bcc": [{
                            "raw": recipient.get("raw", ""),
                            "email": recipient.get("email", ""),
                            "firstName": recipient.get("firstName", ""),
                            "lastName": recipient.get("lastName", "")
                        } for recipient in metadata.get("bcc", [])],
                        "sender": {
                            "email": metadata.get("sender", {}).get("email", "")
                        },
                        "body": metadata.get("text", "") or metadata.get("html", "")
                    }
                elif engagement_data.get("type") == "TASK":
                    formatted_engagement["content"] = {
                        "subject": metadata.get("subject", ""),
                        "body": metadata.get("body", ""),
                        "status": metadata.get("status", ""),
                        "for_object_type": metadata.get("forObjectType", "")
                    }
                elif engagement_data.get("type") == "MEETING":
                    formatted_engagement["content"] = {
                        "title": metadata.get("title", ""),
                        "body": metadata.get("body", ""),
                        "start_time": metadata.get("startTime"),
                        "end_time": metadata.get("endTime"),
                        "internal_notes": metadata.get("internalMeetingNotes", "")
                    }
                elif engagement_data.get("type") == "CALL":
                    formatted_engagement["content"] = {
                        "body": metadata.get("body", ""),
                        "from_number": metadata.get("fromNumber", ""),
                        "to_number": metadata.get("toNumber", ""),
                        "duration_ms": metadata.get("durationMilliseconds"),
                        "status": metadata.get("status", ""),
                        "disposition": metadata.get("disposition", "")
                    }
                
                formatted_engagements.append(formatted_engagement)
            
            # Convert any datetime fields and return
            converted_engagements = convert_datetime_fields(formatted_engagements)
            return json.dumps(converted_engagements)
            
        except ApiException as e:
            logger.error(f"API Exception: {str(e)}")
            return json.dumps({"error": str(e)})
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            return json.dumps({"error": str(e)})

    def get_recent_conversations(self, limit: int = 10) -> str:
        """Get recent conversations from HubSpot inbox
        
        Args:
            limit: Maximum number of conversations to return (default: 10)
        """
        try:
            # Get conversations from inbox API
            inboxes_response = self.client.api_request({
                "method": "GET",
                "path": "/conversations/v3/conversations/inboxes",
                "params": {
                    "limit": limit
                }
            }).json()
            
            formatted_inboxes = []
            
            for inbox in inboxes_response.get('results', []):
                formatted_inbox = {
                    "id": inbox.get("id"),
                    "type": inbox.get("type"),
                    "name": inbox.get("name"),
                    "created_at": inbox.get("createdAt"),
                    "updated_at": inbox.get("updatedAt"),
                    "archived": inbox.get("archived"),
                    "archived_at": inbox.get("archivedAt")
                }
                
                # Get channels for this inbox
                try:
                    # Get channels from channel accounts endpoint
                    channels_response = self.client.api_request({
                        "method": "GET",
                        "path": "/conversations/v3/conversations/channel-accounts",
                        "params": {
                            "inboxId": inbox.get('id')
                        }
                    }).json()
                    
                    formatted_channels = []
                    for channel in channels_response.get('results', []):
                        formatted_channel = {
                            "id": channel.get("id"),
                            "name": channel.get("name"),
                            "channel_id": channel.get("channelId"),
                            "inbox_id": channel.get("inboxId"),
                            "created_at": channel.get("createdAt"),
                            "archived_at": channel.get("archivedAt"),
                            "archived": channel.get("archived"),
                            "authorized": channel.get("authorized"),
                            "active": channel.get("active"),
                            "delivery_identifier": channel.get("deliveryIdentifier")
                        }
                        formatted_channels.append(formatted_channel)
                    
                    formatted_inbox["channels"] = formatted_channels
                except Exception as ch_err:
                    logger.warning(f"Failed to fetch channels for inbox {inbox.get('id')}: {str(ch_err)}")
                    formatted_inbox["channels"] = []
                
                # Get conversations for this inbox
                try:
                    # Get conversations from inbox
                    conversations_response = self.client.api_request({
                        "method": "GET",
                        "path": "/conversations/v3/conversations",
                        "params": {
                            "inboxId": inbox.get('id'),
                            "limit": limit
                        }
                    }).json()
                    
                    conversations = []
                    for conversation in conversations_response.get('results', []):
                        # Format basic conversation details
                        formatted_convo = {
                            "id": conversation.get("id"),
                            "type": conversation.get("type"),
                            "status": conversation.get("status"),
                            "created_time": conversation.get("createdTime"),
                            "last_updated_time": conversation.get("updatedTime"),
                            "subject": conversation.get("subject", ""),
                            "associated_contacts": [
                                {"id": contact.get("id"), "type": contact.get("type")} 
                                for contact in conversation.get("associatedObjects", {}).get("contacts", [])
                            ],
                            "associated_companies": [
                                {"id": company.get("id"), "type": company.get("type")} 
                                for company in conversation.get("associatedObjects", {}).get("companies", [])
                            ]
                        }
                        
                        # Get messages for this conversation
                        try:
                            messages_response = self.client.api_request({
                                "method": "GET",
                                "path": f"/conversations/v3/conversations/{conversation.get('id')}/messages"
                            }).json()
                            
                            messages = []
                            for message in messages_response.get('results', []):
                                formatted_message = {
                                    "id": message.get("id"),
                                    "type": message.get("type"),
                                    "status": message.get("status"),
                                    "created_time": message.get("createdTime"),
                                    "text": message.get("text", ""),
                                    "sender": {
                                        "id": message.get("sender", {}).get("id"),
                                        "type": message.get("sender", {}).get("type"),
                                        "email": message.get("sender", {}).get("email")
                                    },
                                    "recipient": {
                                        "id": message.get("recipient", {}).get("id"),
                                        "type": message.get("recipient", {}).get("type")
                                    }
                                }
                                messages.append(formatted_message)
                            
                            formatted_convo["messages"] = messages
                        except Exception as message_err:
                            logger.warning(f"Failed to fetch messages for conversation {conversation.get('id')}: {str(message_err)}")
                            formatted_convo["messages"] = []
                        
                        conversations.append(formatted_convo)
                    
                    formatted_inbox["conversations"] = conversations
                except Exception as conv_err:
                    logger.warning(f"Failed to fetch conversations for inbox {inbox.get('id')}: {str(conv_err)}")
                    formatted_inbox["conversations"] = []
                
                formatted_inboxes.append(formatted_inbox)
            
            # Convert any datetime fields and return
            converted_inboxes = convert_datetime_fields(formatted_inboxes)
            return json.dumps(converted_inboxes)
            
        except ApiException as e:
            logger.error(f"API Exception: {str(e)}")
            return json.dumps({"error": str(e)})
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            return json.dumps({"error": str(e)})

    def get_channel_accounts(self) -> str:
        """Get channel accounts from HubSpot
        
        Returns:
            JSON string of channel accounts data
        """
        try:
            # Get channel accounts
            channels_response = self.client.api_request({
                "method": "GET",
                "path": "/conversations/v3/conversations/channel-accounts"
            }).json()
            
            # Convert any datetime fields and return
            converted_channels = convert_datetime_fields(channels_response.get('results', []))
            return json.dumps(converted_channels)
            
        except ApiException as e:
            logger.error(f"API Exception: {str(e)}")
            return json.dumps({"error": str(e)})
        except Exception as e:
            logger.error(f"Exception: {str(e)}")
            return json.dumps({"error": str(e)}) 