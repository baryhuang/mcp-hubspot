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
                name="hubspot_get_recent_conversations",
                description="Get recent conversations from HubSpot inbox",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "description": "Maximum number of conversations to return (default: 10)"}
                    },
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

            elif name == "hubspot_get_recent_conversations":
                # Extract parameters with defaults if not provided
                limit = arguments.get("limit", 10) if arguments else 10
                
                # Ensure limit is an integer
                limit = int(limit) if limit is not None else 10
                
                # Get recent conversations
                results = hubspot.get_recent_conversations(limit=limit)
                return [types.TextContent(type="text", text=results)]

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