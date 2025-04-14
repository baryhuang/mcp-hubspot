import logging
from typing import Any, Dict, List, Optional
import os
from dotenv import load_dotenv
from hubspot import HubSpot
from hubspot.crm.contacts import SimplePublicObjectInputForCreate
from hubspot.crm.contacts.exceptions import ApiException
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
from pydantic import AnyUrl
import json
from datetime import datetime
import argparse

from mcp_server_hubspot.client import HubSpotClient

logger = logging.getLogger('mcp_hubspot_server')

async def main(access_token: Optional[str] = None):
    """Run the HubSpot MCP server."""
    logger.info("Server starting")
    hubspot = HubSpotClient(access_token)
    server = Server("hubspot-manager")

    @server.list_resources()
    async def handle_list_resources() -> list[types.Resource]:
        return []

    @server.read_resource()
    async def handle_read_resource(uri: AnyUrl) -> str:
        if uri.scheme != "hubspot":
            raise ValueError(f"Unsupported URI scheme: {uri.scheme}")

        path = str(uri).replace("hubspot://", "")
        return ""

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        """List available tools"""
        return [
            types.Tool(
                name="hubspot_create_contact",
                description="Create a new contact in HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "firstname": {"type": "string", "description": "Contact's first name"},
                        "lastname": {"type": "string", "description": "Contact's last name"},
                        "email": {"type": "string", "description": "Contact's email address"},
                        "properties": {"type": "object", "description": "Additional contact properties"}
                    },
                    "required": ["firstname", "lastname"]
                },
            ),
            types.Tool(
                name="hubspot_create_company",
                description="Create a new company in HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Company name"},
                        "properties": {"type": "object", "description": "Additional company properties"}
                    },
                    "required": ["name"]
                },
            ),
            types.Tool(
                name="hubspot_get_company_activity",
                description="Get activity history for a specific company",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "company_id": {"type": "string", "description": "HubSpot company ID"}
                    },
                    "required": ["company_id"]
                },
            ),
            types.Tool(
                name="hubspot_get_recent_engagements",
                description="Get recent engagement activities across all contacts and companies",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "days": {"type": "integer", "description": "Number of days to look back (default: 7)"},
                        "limit": {"type": "integer", "description": "Maximum number of engagements to return (default: 50)"}
                    },
                },
            ),
            types.Tool(
                name="hubspot_get_active_companies",
                description="Get most recently active companies from HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "description": "Maximum number of companies to return (default: 10)"}
                    },
                },
            ),
            types.Tool(
                name="hubspot_get_active_contacts",
                description="Get most recently active contacts from HubSpot",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "description": "Maximum number of contacts to return (default: 10)"}
                    },
                },
            ),
            types.Tool(
                name="hubspot_conversations_recent",
                description="Get recent conversations from HubSpot inbox",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "description": "Maximum number of conversations to return (default: 10)"}
                    },
                },
            ),
            types.Tool(
                name="hubspot_conversations_inboxes",
                description="Get HubSpot conversation inboxes",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "description": "Maximum number of inboxes to return (default: 10)"}
                    },
                },
            ),
            types.Tool(
                name="hubspot_conversations_channels_for_inbox",
                description="Get channels for a specific HubSpot inbox",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "inbox_id": {"type": "string", "description": "The ID of the inbox to get channels for"}
                    },
                    "required": ["inbox_id"]
                },
            ),
            types.Tool(
                name="hubspot_conversations_all_channels",
                description="Get all HubSpot channel accounts",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="hubspot_conversations_threads_for_inbox",
                description="Get conversation threads for a specific inbox",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "inbox_id": {"type": "string", "description": "The ID of the inbox to get threads for"},
                        "limit": {"type": "integer", "description": "Maximum number of threads to return (default: 10)"}
                    },
                    "required": ["inbox_id"]
                },
            ),
            types.Tool(
                name="hubspot_conversations_threads_for_channel",
                description="Get conversation threads for a specific channel",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "channel_id": {"type": "string", "description": "The ID of the channel to get threads for"},
                        "limit": {"type": "integer", "description": "Maximum number of threads to return (default: 10)"}
                    },
                    "required": ["channel_id"]
                },
            ),
            types.Tool(
                name="hubspot_conversations_thread_messages",
                description="Get messages from a conversation thread",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "thread_id": {"type": "string", "description": "The ID of the thread to get messages from"},
                        "limit": {"type": "integer", "description": "Maximum number of messages to return (default: 10)"}
                    },
                    "required": ["thread_id"]
                },
            ),
            types.Tool(
                name="hubspot_conversations_thread_latest_message",
                description="Get the latest message from a conversation thread",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "thread_id": {"type": "string", "description": "The ID of the thread to get the latest message from"}
                    },
                    "required": ["thread_id"]
                },
            ),
        ]

    @server.call_tool()
    async def handle_call_tool(
        name: str, arguments: dict[str, Any] | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        """Handle tool execution requests"""
        try:
            if name == "hubspot_create_contact":
                if not arguments:
                    raise ValueError("Missing arguments for create_contact")
                
                firstname = arguments["firstname"]
                lastname = arguments["lastname"]
                company = arguments.get("properties", {}).get("company")
                
                # Search for existing contacts with same name and company
                try:
                    from hubspot.crm.contacts import PublicObjectSearchRequest
                    
                    filter_groups = [{
                        "filters": [
                            {
                                "propertyName": "firstname",
                                "operator": "EQ",
                                "value": firstname
                            },
                            {
                                "propertyName": "lastname",
                                "operator": "EQ",
                                "value": lastname
                            }
                        ]
                    }]
                    
                    # Add company filter if provided
                    if company:
                        filter_groups[0]["filters"].append({
                            "propertyName": "company",
                            "operator": "EQ",
                            "value": company
                        })
                    
                    search_request = PublicObjectSearchRequest(
                        filter_groups=filter_groups
                    )
                    
                    search_response = hubspot.client.crm.contacts.search_api.do_search(
                        public_object_search_request=search_request
                    )
                    
                    if search_response.total > 0:
                        # Contact already exists
                        return [types.TextContent(
                            type="text", 
                            text=f"Contact already exists: {search_response.results[0].to_dict()}"
                        )]
                    
                    # If no existing contact found, proceed with creation
                    properties = {
                        "firstname": firstname,
                        "lastname": lastname
                    }
                    
                    # Add email if provided
                    if "email" in arguments:
                        properties["email"] = arguments["email"]
                    
                    # Add any additional properties
                    if "properties" in arguments:
                        properties.update(arguments["properties"])
                    
                    # Create contact using SimplePublicObjectInputForCreate
                    simple_public_object_input = SimplePublicObjectInputForCreate(
                        properties=properties
                    )
                    
                    api_response = hubspot.client.crm.contacts.basic_api.create(
                        simple_public_object_input_for_create=simple_public_object_input
                    )
                    return [types.TextContent(type="text", text=str(api_response.to_dict()))]
                    
                except ApiException as e:
                    return [types.TextContent(type="text", text=f"HubSpot API error: {str(e)}")]

            elif name == "hubspot_create_company":
                if not arguments:
                    raise ValueError("Missing arguments for create_company")
                
                company_name = arguments["name"]
                
                # Search for existing companies with same name
                try:
                    from hubspot.crm.companies import PublicObjectSearchRequest
                    
                    search_request = PublicObjectSearchRequest(
                        filter_groups=[{
                            "filters": [
                                {
                                    "propertyName": "name",
                                    "operator": "EQ",
                                    "value": company_name
                                }
                            ]
                        }]
                    )
                    
                    search_response = hubspot.client.crm.companies.search_api.do_search(
                        public_object_search_request=search_request
                    )
                    
                    if search_response.total > 0:
                        # Company already exists
                        return [types.TextContent(
                            type="text", 
                            text=f"Company already exists: {search_response.results[0].to_dict()}"
                        )]
                    
                    # If no existing company found, proceed with creation
                    properties = {
                        "name": company_name
                    }
                    
                    # Add any additional properties
                    if "properties" in arguments:
                        properties.update(arguments["properties"])
                    
                    # Create company using SimplePublicObjectInputForCreate
                    simple_public_object_input = SimplePublicObjectInputForCreate(
                        properties=properties
                    )
                    
                    api_response = hubspot.client.crm.companies.basic_api.create(
                        simple_public_object_input_for_create=simple_public_object_input
                    )
                    return [types.TextContent(type="text", text=str(api_response.to_dict()))]
                    
                except ApiException as e:
                    return [types.TextContent(type="text", text=f"HubSpot API error: {str(e)}")]

            elif name == "hubspot_get_company_activity":
                if not arguments:
                    raise ValueError("Missing arguments for get_company_activity")
                results = hubspot.get_company_activity(arguments["company_id"])
                return [types.TextContent(type="text", text=results)]
                
            elif name == "hubspot_get_recent_engagements":
                # Extract parameters with defaults if not provided
                days = arguments.get("days", 7) if arguments else 7
                limit = arguments.get("limit", 50) if arguments else 50
                
                # Ensure days and limit are integers
                days = int(days) if days is not None else 7
                limit = int(limit) if limit is not None else 50
                
                # Get recent engagements
                results = hubspot.get_recent_engagements(days=days, limit=limit)
                return [types.TextContent(type="text", text=results)]

            elif name == "hubspot_get_active_companies":
                # Extract parameters with defaults if not provided
                limit = arguments.get("limit", 10) if arguments else 10
                
                # Ensure limit is an integer
                limit = int(limit) if limit is not None else 10
                
                # Get recent companies
                results = hubspot.get_recent_companies(limit=limit)
                return [types.TextContent(type="text", text=results)]

            elif name == "hubspot_get_active_contacts":
                # Extract parameters with defaults if not provided
                limit = arguments.get("limit", 10) if arguments else 10
                
                # Ensure limit is an integer
                limit = int(limit) if limit is not None else 10
                
                # Get recent contacts
                results = hubspot.get_recent_contacts(limit=limit)
                return [types.TextContent(type="text", text=results)]

            elif name == "hubspot_conversations_recent":
                # Extract parameters with defaults if not provided
                limit = arguments.get("limit", 10) if arguments else 10
                
                # Ensure limit is an integer
                limit = int(limit) if limit is not None else 10
                
                # Get recent conversations
                results = hubspot.get_recent_conversations(limit=limit)
                return [types.TextContent(type="text", text=results)]

            # New modular tools for inbox, channels, threads and messages
            elif name == "hubspot_conversations_inboxes":
                # Extract parameters with defaults
                limit = arguments.get("limit", 10) if arguments else 10
                limit = int(limit) if limit is not None else 10
                
                # Get inboxes
                inboxes = hubspot.get_inboxes(limit=limit)
                return [types.TextContent(type="text", text=json.dumps(inboxes))]
                
            elif name == "hubspot_conversations_channels_for_inbox":
                if not arguments or "inbox_id" not in arguments:
                    raise ValueError("Missing inbox_id argument")
                    
                # Get channels for inbox
                channels = hubspot.get_channels_for_inbox(inbox_id=arguments["inbox_id"])
                return [types.TextContent(type="text", text=json.dumps(channels))]
                
            elif name == "hubspot_conversations_all_channels":
                # Get all channels
                channels = hubspot.get_all_channels()
                return [types.TextContent(type="text", text=json.dumps(channels))]
                
            elif name == "hubspot_conversations_threads_for_inbox":
                if not arguments or "inbox_id" not in arguments:
                    raise ValueError("Missing inbox_id argument")
                    
                # Extract optional limit parameter
                limit = arguments.get("limit", 10)
                limit = int(limit) if limit is not None else 10
                
                # Get threads for inbox
                threads = hubspot.get_threads_for_inbox(inbox_id=arguments["inbox_id"], limit=limit)
                return [types.TextContent(type="text", text=json.dumps(threads))]
                
            elif name == "hubspot_conversations_threads_for_channel":
                if not arguments or "channel_id" not in arguments:
                    raise ValueError("Missing channel_id argument")
                    
                # Extract optional limit parameter
                limit = arguments.get("limit", 10)
                limit = int(limit) if limit is not None else 10
                
                # Get threads for channel
                threads = hubspot.get_threads_for_channel(channel_id=arguments["channel_id"], limit=limit)
                return [types.TextContent(type="text", text=json.dumps(threads))]
                
            elif name == "hubspot_conversations_thread_messages":
                if not arguments or "thread_id" not in arguments:
                    raise ValueError("Missing thread_id argument")
                    
                # Extract optional limit parameter
                limit = arguments.get("limit", 10)
                limit = int(limit) if limit is not None else 10
                
                # Get messages for thread
                messages = hubspot.get_thread_messages(thread_id=arguments["thread_id"], limit=limit)
                return [types.TextContent(type="text", text=json.dumps(messages))]
                
            elif name == "hubspot_conversations_thread_latest_message":
                if not arguments or "thread_id" not in arguments:
                    raise ValueError("Missing thread_id argument")
                    
                # Get latest message for thread
                message = hubspot.get_thread_latest_message(thread_id=arguments["thread_id"])
                return [types.TextContent(type="text", text=json.dumps(message))]

            else:
                raise ValueError(f"Unknown tool: {name}")

        except ApiException as e:
            return [types.TextContent(type="text", text=f"HubSpot API error: {str(e)}")]
        except Exception as e:
            return [types.TextContent(type="text", text=f"Error: {str(e)}")]

    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        logger.info("Server running with stdio transport")
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="hubspot",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    import asyncio
    import argparse
    
    # Set up command line argument parser
    parser = argparse.ArgumentParser(description="Run the HubSpot MCP server")
    parser.add_argument("--access-token", 
                        help="HubSpot API access token (overrides HUBSPOT_ACCESS_TOKEN environment variable)")
    
    args = parser.parse_args()
    asyncio.run(main(access_token=args.access_token)) 