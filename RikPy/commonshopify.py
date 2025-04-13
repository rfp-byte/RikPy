import requests
import re
from requests_toolbelt.multipart.encoder import MultipartEncoder
from .customresponse import CustomResponse
from datetime import datetime, timezone
from .commonfunctions import rfplogger, download_file_local, delete_local_file
import time
import json
import os
from dotenv import load_dotenv
import random

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

##### Prepare the GraphQL MUTATIONS
rik = 1

queryPublicationID='''
    {
      publications(first: 5) {
        edges {
          node {
            id
            name
          }
        }
      }
    }
    '''

mutationstagedUploadsCreate = '''
    mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
    stagedUploadsCreate(input: $input) {
        stagedTargets {
        url
        resourceUrl
        parameters {
            name
            value
        }
        }
        userErrors {
        field
        message
        }
    }
    }
    '''

mutationbulkOperationRunMutation = '''
    mutation bulkOperationRunMutation($mutation: String!, $stagedUploadPath: String!) {
    bulkOperationRunMutation(mutation: $mutation, stagedUploadPath: $stagedUploadPath) {
        bulkOperation {
            id
            status
        }
            userErrors {
                field
                message
        }
    }
    }
    '''

###################### AUX FUNCTIONS

def get_mime_type(file_extension):
    mime_types = {
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "gif": "image/gif",
        "bmp": "image/bmp",
        # Add more types as needed
    }
    return mime_types.get(file_extension.lower(), "application/octet-stream")

def get_file_extension(mime_type):
    mime_to_extension = {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/gif": "gif",
        "image/bmp": "bmp",
        "image/webp": "webp",
        "image/tiff": "tiff",
        "image/x-icon": "ico",
        # Add more MIME types and their corresponding file extensions as needed
    }
    return mime_to_extension.get(mime_type.lower(), "")

def verify_token(shop="", access_token="", api_version="2024-01"):
    """
    Verify if the Shopify access token is valid by making a simple API call.
    
    Args:
        shop (str): The shop's myshopify domain
        access_token (str): The shop's access token
        api_version (str): Shopify API version to use
        
    Returns:
        bool: True if token is valid, False otherwise
    """
    url = f"https://{shop}/admin/api/{api_version}/shop.json"
    headers = {'X-Shopify-Access-Token': access_token}
    
    try:
        response = requests.get(url, headers=headers)
        print("Token verification status code:", response.status_code)
        
        if response.status_code != 200:
            print("Token verification failed. Response:", response.text[:200])
            return False
            
        return True
        
    except Exception as e:
        print(f"Error verifying token: {str(e)}")
        return False

######################### GRAPHQL FUNCTIONS

def Shopify_get_metaobject_gid(shop="", access_token="", api_version="2024-01", metaobject_type="", handle=""):

    # print(f"Access token: {access_token}")

    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    # print(f"headers: {headers}")

    query = """
    query GetMetaobjectByHandle($type: String!, $handle: String!) {
      metaobjectByHandle(handle: {
            type: $type,
            handle: $handle
        }) {
            id
            type
            handle
        }
    }
    """
    
    variables = {
        "type": metaobject_type,
        "handle": handle
    }
    
    payload = {
        'query': query,
        'variables': variables
    }

    # print(f"payload: {payload}")
    
    response = requests.post(url, json=payload, headers=headers)
    # response = requests.post(url, json={'query': query}, headers=headers)
    
    if response.status_code == 200:
        try:
            response_json = response.json()
            # Attempt to access the nested dictionary keys
            result_id = response_json['data']['metaobjectByHandle']['id']
            return result_id
        except TypeError as e:
            # Handle the case where the expected structure is not present
            print(f"TypeError occurred: {e}")
            print("The response structure was not as expected:", response_json)
            return None
        except KeyError as e:
            # Handle the case where a key is missing
            print(f"KeyError occurred: {e}")
            print("The response structure was not as expected:", response_json)
            return None
    else:
        print(f"Error: {response.status_code}")
        return None

def Shopify_update_metaobject(shop, access_token, api_version="2024-01", metaobject_gid="", banner_url="", mobile_banner_url="", 
                              product_url="", banner_title="", banner_subtitle="", button_text="", button_url="", metaobject_banner_number=1):
    
    if not shop:
        return CustomResponse(data="Missing shop arguement.", status_code=400)
    if not access_token:
        return CustomResponse(data="Missing access_token arguement.", status_code=400)
    
    # Push to shopify banner object for vinzo
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    # Generate field names based on metaobject_banner_number
    field_names = [f"product_link_{metaobject_banner_number}",
                   f"banner_url_{metaobject_banner_number}",
                   f"mobile_banner_url_{metaobject_banner_number}",
                   f"banner_title_{metaobject_banner_number}",
                   f"banner_subtitle_{metaobject_banner_number}",
                   f"button_text_{metaobject_banner_number}",
                   f"button_url_{metaobject_banner_number}"
    ]

    mutation = """
    mutation UpdateMetaobject($id: ID!, $metaobject: MetaobjectUpdateInput!) {
    metaobjectUpdate(id: $id, metaobject: $metaobject) {
        metaobject {
        handle
        """
    
    # Add dynamic field names to the mutation
    for field_name in field_names:
        mutation += f"{field_name}: field(key: \"{field_name}\") {{ value }}\n"

    mutation += """
        }
        userErrors {
        field
        message
        code
        }
    }
    }
    """

    variables = { 
        "id": metaobject_gid,
        "metaobject": {
            "fields": [
                {"key": field_name, "value": value}
                for field_name, value in zip(field_names, [product_url, banner_url, mobile_banner_url, banner_title, banner_subtitle, button_text, button_url])
            ]
        } 
    }

    response = requests.post(url, json={'query': mutation, 'variables': variables}, headers=headers)
    
    if response.status_code == 200:
        return CustomResponse(data=response.json(), status_code=200)
    else:
        message=f"Error loading to shopify: {response.status_code}"
        print(message)
        return CustomResponse(data=message, status_code=response.status_code)

def Shopify_get_products(shop="", access_token="", api_version="2024-01", number_products=0):

    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/products.json?limit=250"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    all_products = []
    i = 0
    while url:
        if number_products != 0 and i*250 > number_products: break
        i+=1
        print(i)
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            message=f"Failed to retrieve products: {response.text}"
            print(message)
            return CustomResponse(data=message, status_code=response.status_code)
        
        products=response.json()['products']
        all_products.extend(products)
        links = response.headers.get('Link', None)
        next_url = None
        if links:
            for link in links.split(','):
                if 'rel="next"' in link:
                    next_url = link.split(';')[0].strip('<>')
                    next_url = next_url.strip('<> ')
                    break
            url = next_url if next_url else None
        else:
            break
        
    return CustomResponse(data=all_products, status_code=200)
    
def Shopify_get_collections(shop="", access_token="", api_version="2024-01"):

    url_custom = f"https://{shop}.myshopify.com/admin/api/{api_version}/custom_collections.json"
    url_smart = f"https://{shop}.myshopify.com/admin/api/{api_version}/smart_collections.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    response = requests.get(url_smart, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve smart collections: {response.status_code}")
        print(f"response {response.text}")
        return CustomResponse(data=response.text, status_code=response.status_code)
    smart_collections = response.json()['smart_collections']
    
    response = requests.get(url_custom, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve custom collections: {response.status_code}")
        print(f"response {response.text}")
        return CustomResponse(data=response.text, status_code=response.status_code)
    custom_collections = response.json()['custom_collections']
    
    all_collections = smart_collections + custom_collections

    return CustomResponse(data=all_collections, status_code=200)

def Shopify_get_collection_metadata(shop="", access_token="", api_version="2024-01", collection_id=""):
    '''Returns metafields and metadata'''
    metadata_url = f"https://{shop}.myshopify.com/admin/api/{api_version}/collections/{collection_id}.json"
    
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    response = requests.get(metadata_url, headers=headers)

    if response.status_code != 200:
        print(f"Failed to retrieve metadata for collection ID {collection_id}. Status code: {response.status_code}")
        print(f"Response: {response.text}")
        return CustomResponse(data=response.text, status_code=400)
    
    collection_metadata = response.json()['collection']
    
    # Retrieve metafields for the collection
    metafields_url = f"https://{shop}.myshopify.com/admin/api/{api_version}/collections/{collection_id}/metafields.json"
    response = requests.get(metafields_url, headers=headers)

    if response.status_code != 200:
        print(f"Failed to retrieve metafields for collection ID {collection_id}. Status code: {response.status_code}")
        print(f"Metafields response: {response.text}")
        return CustomResponse(data=response.text, status_code=400)
    metafields_data = response.json()['metafields']

    # Join metadata and metafield 
    collection_metadata['metafields'] = metafields_data

    # print(f"collection_metadata: {collection_metadata}")

    return CustomResponse(data=collection_metadata, status_code=200)

def Shopify_get_collection_url(shop="", access_token="", api_version="2024-01", collection_id=""):    
    collection_url = f"https://{shop}.myshopify.com/admin/api/{api_version}/collections/{collection_id}"
    response = requests.get(collection_url)    
    if response.status_code == 200:
        return CustomResponse(data=collection_url, status_code=200)
    else:
        # Handle the case where the URL does not exist
        return CustomResponse(data="Collection URL does not exist", status_code=404)

def Shopify_get_products_in_collection(shop="", access_token="", api_version="2024-01", collection_id=""):
    '''
    status parameter: I've added a status parameter to the API request query string: ?status={status}. This allows you to filter products based on their status.
    active: Retrieves only active products.
    archived: Retrieves only archived products.
    any: Retrieves both active and archived products (default).
    '''

    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/collections/{collection_id}/products.json"

    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    all_products = []

    while url:

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            products=response.json()['products']
            all_products.extend(products)
            links = response.headers.get('Link', None)

            next_url = None
            if links:
                for link in links.split(','):
                    if 'rel="next"' in link:
                        next_url = link.split(';')[0].strip('<>')
                        next_url = next_url.strip('<> ')
                        break
                url = next_url if next_url else None
            else:
                break


        else:
            print(f"Failed to retrieve products in collection {collection_id}: {response.status_code}")
            return CustomResponse(data=response.text, status_code=400)

    return CustomResponse(data=all_products, status_code=200)

class ShopifyRateLimiter:
    def __init__(self, max_requests_per_second=2):
        self.max_requests_per_second = max_requests_per_second
        self.last_request_time = 0
        self.retry_count = 0
        self.max_retries = 5
        self.base_delay = 1  # Base delay in seconds

    def wait(self):
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < 1/self.max_requests_per_second:
            time.sleep(1/self.max_requests_per_second - time_since_last_request)
        self.last_request_time = time.time()

    def handle_throttle(self):
        self.retry_count += 1
        if self.retry_count > self.max_retries:
            raise Exception("Max retries exceeded")
        
        # Exponential backoff with jitter
        delay = self.base_delay * (2 ** (self.retry_count - 1)) + random.uniform(0, 1)
        time.sleep(delay)
        return True

    def reset_retry_count(self):
        self.retry_count = 0

def Shopify_get_products_query(shop="", access_token="", api_version="2024-01"):
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    # Initialize variables for pagination
    cursor = None
    filtered_products = []
    rate_limiter = ShopifyRateLimiter()
    
    i = 0
    while True:
        print(f"[Request {i+1}] Getting products from shopify...")
        i += 1
        
        # Construct GraphQL query with pagination
        query = '''
        query ($cursor: String) {
            products(first: 250, after: $cursor) {
                edges {
                    node {
                        id
                        title
                        bodyHtml
                        vendor
                        productType
                        createdAt
                        handle
                        updatedAt
                        publishedAt
                        tags
                        status
                        variants(first: 250) {
                            edges {
                                node {
                                    id
                                    title
                                    price
                                    sku
                                    position
                                    inventoryPolicy
                                    compareAtPrice
                                    inventoryManagement
                                    createdAt
                                    updatedAt
                                    taxable
                                    barcode
                                    weight
                                    weightUnit
                                    inventoryQuantity
                                    requiresShipping
                                }
                            }
                        }
                        options {
                            id
                            name
                            values
                        }
                        images(first: 250) {
                            edges {
                                node {
                                    id
                                    src
                                    altText
                                    width
                                    height
                                }
                            }
                        }
                    }
                    cursor
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        '''

        # Apply rate limiting
        rate_limiter.wait()
        
        # Send request to Shopify GraphQL API
        response = requests.post(url, json={'query': query, 'variables': {'cursor': cursor}}, headers=headers)

        if response.status_code != 200:
            error_message = f"Failed to retrieve products: {response.status_code}"
            print(f"[Error] {error_message}")
            return CustomResponse(data=error_message, status_code=400)
        
        response_json = response.json()
        
        # Check for GraphQL errors
        if 'errors' in response_json:
            error_message = f"GraphQL Error: {response_json['errors']}"
            print(f"[Error] {error_message}")
            
            # Check if it's a throttling error
            if any(error.get('extensions', {}).get('code') == 'THROTTLED' for error in response_json['errors']):
                print(f"[Rate Limiter] Throttling detected. Retry count: {rate_limiter.retry_count}")
                if rate_limiter.handle_throttle():
                    print(f"[Rate Limiter] Retrying after backoff...")
                    continue  # Retry the request
                else:
                    print(f"[Rate Limiter] Max retries ({rate_limiter.max_retries}) exceeded")
                    return CustomResponse(data=error_message, status_code=429)
            
            return CustomResponse(data=error_message, status_code=400)

        if 'data' not in response_json:
            error_message = "No data in response"
            print(f"[Error] {error_message}")
            return CustomResponse(data=error_message, status_code=400)

        products = response_json['data']['products']['edges']
        page_info = response_json['data']['products']['pageInfo']
        cursor = page_info['endCursor'] if page_info['hasNextPage'] else None

        # BUILD THE PRODUCT OBJECT
        for edge in products:
            node = edge['node']
            # Construct a product dictionary with the required fields
            product_dict = {
                'id': node['id'],
                'title': node['title'],
                'body_html': node.get('bodyHtml', ''),
                'vendor': node.get('vendor', ''),
                'product_type': node.get('productType', ''),
                'created_at': node.get('createdAt', ''),
                'handle': node.get('handle', ''),
                'updated_at': node.get('updatedAt', ''),
                'published_at': node.get('publishedAt', ''),
                'template_suffix': node.get('templateSuffix', None),
                'published_scope': node.get('publishedScope', ''),
                'tags': node.get('tags', ''),
                'status': node.get('status', ''),
                'admin_graphql_api_id': node.get('adminGraphqlApiId', ''),
                'variants': [{'id': variant['node']['id'],
                            'product_id': node['id'],
                            'title': variant['node'].get('title', ''),
                            'price': variant['node'].get('price', ''),
                            # Add other variant fields here as needed
                            } for variant in node.get('variants', {}).get('edges', [])],
                'options': node.get('options', []),
                'images': node.get('images', {}).get('edges', []),
                'image': node.get('image', {})
            }

            filtered_products.append(product_dict)

        print(f"[Success] Retrieved {len(products)} products in this batch")
        rate_limiter.reset_retry_count()  # Reset retry count on successful request

        if not page_info['hasNextPage']:
            print(f"[Complete] No more pages to fetch")
            break

    print(f"[Complete] Total products retrieved: {len(filtered_products)}")

    return CustomResponse(data=filtered_products, status_code=200)

def Shopify_get_product_variants(shop="", access_token="", api_version="2024-01", product_id=""):
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/products/{product_id}/variants.json"
    
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        variants=response.json()['variants']
        return CustomResponse(data=variants, status_code=200)
    else:
        print(f"Failed to retrieve product variants for product {product_id}: {response.status_code}")
        return CustomResponse(data=response.text, status_code=400)

def Shopify_get_product_variants_mutation(shop="", access_token="", api_version="2024-01", product_id=""):
    
    graphql_query = '''
    {
      product(id: "gid://shopify/Product/%s") {
        variants(first: 250) {
          edges {
            node {
              id
              title
              price
              presentmentPrices(first: 1) {
                edges {
                  node {
                    price {
                      amount
                      currencyCode
                    }
                  }
                }
              }
              sku
              position
              inventoryPolicy
              compareAtPrice
              inventoryManagement
              createdAt
              updatedAt
              taxable
              barcode
              weight
              weightUnit
              inventoryQuantity
              requiresShipping
            }
          }
        }
      }
    }
    ''' % product_id
    
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }
    data = {'query': graphql_query}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    if response.status_code != 200:
        print(f"Failed to retrieve product variants for product {product_id}: {response.status_code}")
        return CustomResponse(data=response.text, status_code=400)

    data = response.json()
    variants_data = data['data']['product']['variants']['edges']
    variants_with_currency = []

    for variant_edge in variants_data:
        variant = variant_edge['node']
        currency_code = variant.get('presentmentPrices', {}).get('edges', [{}])[0].get('node', {}).get('price', {}).get('currencyCode')
        variant['currency_code'] = currency_code
        variants_with_currency.append(variant)

    return CustomResponse(data=variants_with_currency, status_code=200)

def Shopify_get_customers(shop="", access_token="", api_version="2024-01"):
    # Endpoint URL for fetching customers
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/customers.json"
    
    # Headers for the request, including the required access token for authentication
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    # Making the GET request to the API
    response = requests.get(url, headers=headers)
    
    # Check the response status code
    if response.status_code != 200:
        # If the request was not successful, print an error message and return a custom response
        print(f"Failed to retrieve customers: {response.status_code}")
        print(f"response: {response.text}")
        return CustomResponse(data=response.text, status_code=response.status_code)
    
    # If the request was successful, parse the JSON response to get the customers
    customers = response.json()['customers']
    
    # Return a custom response containing the customers and a successful status code
    return CustomResponse(data=customers, status_code=200)

def Shopify_get_products_with_metafields(shop="", access_token="", api_version="2024-01", metafield_key="custom.unpublish_after", filterdate="23/02/2024"):
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    # Initialize variables for pagination
    cursor = None
    filtered_products = []
    
    i = 0
    while True:
        print(f"Getting products... {i}", end='\r', flush=True)
        i += 1
        # Construct GraphQL query with pagination
        query = '''
        query ($cursor: String) {
            products(first: 250, after: $cursor) {
                edges {
                    node {
                        id
                        title
                        metafield(key: "%s") {
                            value
                        }
                    }
                    cursor
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        ''' % (metafield_key)

        # Send request to Shopify GraphQL API
        response = requests.post(url, json={'query': query, 'variables': {'cursor': cursor}}, headers=headers)
        
        if response.status_code != 200:
            error_message = f"Failed to retrieve products with metafields: {response.status_code}"
            print(error_message)
            return CustomResponse(data=error_message, status_code=400)
        
        products = response.json()['data']['products']['edges']
        page_info = response.json()['data']['products']['pageInfo']
        cursor = page_info['endCursor'] if page_info['hasNextPage'] else None
              
        for product in products:
            # Attempt to retrieve the 'metafield' if it exists, otherwise use an empty dictionary
            metafield = product['node'].get('metafield') or {}
            metafield_value = metafield.get('value', '')
            
            if metafield_value:
                try:
                    # Convert the metafield value string to a datetime object
                    metafield_date = datetime.strptime(metafield_value, '%Y-%m-%dT%H:%M:%S%z')
                    
                    # Check the format of filterdate and parse accordingly
                    if '/' in filterdate:
                        filter_date = datetime.strptime(filterdate, '%d/%m/%Y').replace(tzinfo=timezone.utc)
                    elif '-' in filterdate:
                        filter_date = datetime.strptime(filterdate, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    else:
                        raise ValueError(f"Unrecognized date format: {filterdate}")
                    
                    if metafield_date < filter_date:
                        filtered_products.append({
                            'id': product['node']['id'],
                            'title': product['node']['title'],
                            'unpublish_metafield': metafield_value
                        })
            
                except ValueError as e:
                    print(f"Error parsing date for product {product['node']['id']}: {e}")

        if not page_info['hasNextPage']:
            break

    return CustomResponse(data=filtered_products, status_code=200)

def Shopify_get_products_and_inventoryid_with_metafields(shop="", access_token="", api_version="2024-01", metafield_key="custom.unpublish_after", filterdate="23/02/2024"):
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    # Initialize variables for pagination
    cursor = None
    filtered_products = []
    
    i = 0
    while True:
        print(f"Getting products and inventory id... {i}", end='\r', flush=True)
        i += 1
        # Construct GraphQL query with pagination and include variant inventory_item_id
        query = '''
        query ($cursor: String) {
            products(first: 250, after: $cursor) {
                edges {
                    node {
                        id
                        title
                        bodyHtml
                        metafield(key: "%s") {
                            value
                        }
                        variants(first: 250) {
                            edges {
                                node {
                                    id
                                    inventoryItem {
                                        id
                                    }
                                }
                            }
                        }
                    }
                    cursor
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
        ''' % (metafield_key)

        # Send request to Shopify GraphQL API
        response = requests.post(url, json={'query': query, 'variables': {'cursor': cursor}}, headers=headers)
        
        if response.status_code != 200:
            error_message = f"Failed to retrieve products with metafields: {response.status_code}"
            print(error_message)
            return CustomResponse(data=error_message, status_code=400)
        
        products = response.json()['data']['products']['edges']
        page_info = response.json()['data']['products']['pageInfo']
        cursor = page_info['endCursor'] if page_info['hasNextPage'] else None

        for product in products:
            # Attempt to retrieve the 'metafield' if it exists, otherwise use an empty dictionary
            metafield = product['node'].get('metafield') or {}
            metafield_value = metafield.get('value', '')
            
            variant_inventory_ids = [variant['node']['inventoryItem']['id'] for variant in product['node']['variants']['edges']]
            
            if metafield_value:
                try:
                    # Convert the metafield value string to a datetime object
                    metafield_date = datetime.strptime(metafield_value, '%Y-%m-%dT%H:%M:%S%z')
                    
                    # Check the format of filterdate and parse accordingly
                    if '/' in filterdate:
                        filter_date = datetime.strptime(filterdate, '%d/%m/%Y').replace(tzinfo=timezone.utc)
                    elif '-' in filterdate:
                        filter_date = datetime.strptime(filterdate, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    else:
                        raise ValueError(f"Unrecognized date format: {filterdate}")
                    
                    if metafield_date < filter_date:
                        filtered_products.append({
                            'id': product['node']['id'],
                            'title': product['node']['title'],
                            'unpublish_metafield': metafield_value,
                            'variant_inventory_item_ids': variant_inventory_ids
                        })
            
                except ValueError as e:
                    print(f"Error parsing date for product {product['node']['id']}: {e}")

        if not page_info['hasNextPage']:
            break
        
        #print("Wait 3 seconds...")
        time.sleep(3) # Query takes 288 tokens, wait 3 seconds so never deplete

    return CustomResponse(data=filtered_products, status_code=200)

def Shopify_unpublish_products_channel(shop="", access_token="", api_version="2024-01", products=[], channel_id=""):
    
    '''
    Upublishes products for a channel id by removing from channel id
    '''

    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    # Prepare the GraphQL mutation for unpublishing products
    mutation = '''
    mutation publishableUnpublish($id: ID!, $input: [PublicationInput!]!) {
      publishableUnpublish(id: $id, input: $input) {
        userErrors {
          field
          message
        }
      }
    }
    '''
    allpublished = True
    for product in products:
        variables = {
            # "id": f"gid://shopify/Product/{product['id']}",
            "id": product['admin_graphql_api_id'],
            "input": [{
                "publicationId": channel_id
            }]
        }
        response = requests.post(url, json={'query': mutation, 'variables': variables}, headers=headers)
        if response.status_code == 200:
            errors = response.json().get('errors', [])  
            if errors:
                allpublished=False
                continue

            # data = response.json().get('data', {})
            # print(f"Product {product['id']} unpublished successfully.")
        else:
            allpublished=False
            print(f"Failed to unpublish product {product['id']}: {response.status_code}")

    message = "All products were unpublished correctly"
    if allpublished != True:
        message = "Not all products were unpublished correctly"

    return CustomResponse(data=message, status_code=200)

def Shopify_get_online_store_channel_id(shop="", access_token="", api_version="2024-01"):
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"

    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token,
    }
    query = '''
    {
      publications(first: 250) {
        edges {
          node {
            id
            name
          }
        }
      }
    }
    '''
    response = requests.post(url, json={'query': query}, headers=headers)
    if response.status_code == 200:
        data = response.json()
        publications = data['data']['publications']['edges']
        for publication in publications:
            if publication['node']['name'] == 'Online Store':
                return publication['node']['id']
    return None

def Shopify_reduce_inventory_by_9999(shop="", access_token="", api_version="2024-01", inventory_item_ids="", location_id=""):
    # inventory_item_ids = ["inventory-item-id-1", "inventory-item-id-2"]  # List of inventory item IDs

    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    # GraphQL mutation to set inventory quantities to zero
    mutation = '''
    mutation inventoryBulkAdjustQuantityAtLocation($inventoryItemAdjustments: [InventoryAdjustItemInput!]!, $locationId: ID!) {
      inventoryBulkAdjustQuantityAtLocation(inventoryItemAdjustments: $inventoryItemAdjustments, locationId: $locationId) {
        inventoryLevels {
          id
          available
        }
        userErrors {
          field
          message
        }
      }
    }
    '''
    # Preparing the adjustments input for the GraphQL mutation
    adjustments = [{"inventoryItemId": item_id, "availableDelta": -9999} for item_id in inventory_item_ids]
    print(adjustments)
    variables = {
        "inventoryItemAdjustments": adjustments,
        "locationId": location_id
        
    }

    response = requests.post(url, json={'query': mutation, 'variables': variables}, headers=headers)
    if response.status_code == 200:
        message="Inventory set to zero successfully."
        print(message)
        return CustomResponse(data=message, status_code=200)
        
    else:
        message=f"Failed to set inventory to zero: {response.status_code}"
        print(message)
        return CustomResponse(data=message, status_code=400)

def Shopify_set_inventory_to_zero(shop="", access_token="", api_version="2024-01", inventory_item_ids="", location_id="", reason="correction", reference_document_uri=""):
    
    # Loop through each chunk and make the API call
    for chunk in chunker(inventory_item_ids, 250):

        url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
        headers = {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': access_token
        }

        # GraphQL mutation to set inventory quantities to zero
        mutation = '''
        mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
        inventorySetOnHandQuantities(input: $input) {
            inventoryAdjustmentGroup {
            id
            }
            userErrors {
            field
            message
            }
        }
        }
        '''
        # Build the setQuantities input dynamically
        #set_quantities = [{"inventoryItemId": item_id, "locationId": location_id, "quantity": 0} for item_id in inventory_item_ids]
        set_quantities = [{"inventoryItemId": item_id, "locationId": location_id, "quantity": 0} for item_id in chunk]

        variables = {
            "input": {
                "reason": reason,
                #"referenceDocumentUri": reference_document_uri,
                "setQuantities": set_quantities
            }
        }

        response = requests.post(url, json={'query': mutation, 'variables': variables}, headers=headers)
        
        if response.status_code != 200:
            message = f"Failed to set inventory to zero for chunk: {response.status_code}"
            print(message)
            return CustomResponse(data=message, status_code=400)

        print("Waiting...")
        time.sleep(2)

    message="Inventory set to zero successfully for all items."
    print(message)
    return CustomResponse(data=message, status_code=200)
    
def Shopify_get_locations(shop="", access_token="", api_version="2024-01"):
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/locations.json"
    headers = {"X-Shopify-Access-Token": access_token}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return CustomResponse(data=response.json()['locations'], status_code=200)  # Returns a list of locations
    else:
        print(f"Failed to retrieve locations: {response.status_code}")
        return CustomResponse(data="", status_code=400)
    
def Shopify_get_publication_id(shop="", access_token="", api_version="2024-01", name="Online Store"):
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }
    
    response = requests.post(url, json={'query': queryPublicationID}, headers=headers)
    publications = response.json().get('data', {}).get('publications', {}).get('edges', [])
    for pub in publications:
        # print(f"Publication ID: {pub['node']['id']}, Name: {pub['node']['name']}")
        # If looking for the default online store publication, you might compare by name
        if pub['node']['name'] == name:
            publication_id = pub['node']['id']
            # print(f"Found Online Store Publication ID: {publication_id}")
    return publication_id

def Shopify_get_publications(shop="", access_token="", api_version="2024-01"):
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    
    query = '''
    {
      publications(first: 5) {
        edges {
          node {
            id
            name
          }
        }
      }
    }
    '''
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }
    response = requests.post(url, json={'query': query}, headers=headers)
    return response.json()

def Shopify_start_bulk_operation(shop, access_token, api_version, products):
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token,
    }

    # Define your bulk operation query here
    # This is a simplified placeholder example
    bulk_operation_query = '''
    mutation {
        bulkOperationRunQuery(
            query: """
            {
                products {
                    edges {
                        node {
                            id
                            title
                            # Add fields to update here
                        }
                    }
                }
            }
            """
        ) {
            bulkOperation {
                id
                status
            }
            userErrors {
                field
                message
            }
        }
    }
    '''

    response = requests.post(url, json={'query': bulk_operation_query}, headers=headers)
    response_json = response.json()

    # Extract the operation ID and return it for polling
    operation_id = response_json['data']['bulkOperationRunQuery']['bulkOperation']['id']
    return operation_id

def Shopify_poll_bulk_operation_status(shop, access_token, api_version, operation_id):
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token,
    }

    check_status_query = '''
    query {
        currentBulkOperation {
            id
            status
            errorCode
            completedAt
            objectCount
            fileSize
            url
            partialDataUrl
        }
    }
    '''

    while True:
        response = requests.post(url, json={'query': check_status_query}, headers=headers)
        response_json = response.json()
        current_operation_id = response_json['data']['currentBulkOperation']['id']
        operation_status = response_json['data']['currentBulkOperation']['status']

        # Check if the current operation ID matches the one we're interested in
        if current_operation_id != operation_id:
            print(f"Current operation ID ({current_operation_id}) does not match the expected operation ID ({operation_id}).")
            # Handle this situation, e.g., by breaking or continuing to poll
            break

        if operation_status == 'COMPLETED':
            results_url = response_json['data']['currentBulkOperation']['url']
            return results_url
        elif operation_status == 'FAILED':
            # Handle failure here
            print("Bulk operation failed.")
            break
        else:
            print("Bulk operation is still processing...")
            time.sleep(10)  # Poll every 10 seconds

def Shopify_archive_products(shop, access_token, api_version, product_ids):
    """
    Archives Shopify products by setting their status to ARCHIVED.

    :param shop: The Shopify store's domain.
    :param access_token: The Shopify API access token.
    :param api_version: The Shopify API version to use.
    :param product_ids: List of product GraphQL IDs to archive.
    """
    print("Starting archive products")
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token,
    }

    # STEP 1: Archive each product individually
    for product_id in product_ids:
        
        mutation_string = """
        mutation productUpdate($input: ProductInput!) {
            productUpdate(input: $input) {
                product {
                    id
                    status
                }
                userErrors {
                    field
                    message
                }
            }
        }
        """
        variables = {
            "input": {
                "id": product_id,
                "status": "ARCHIVED"
            }
        }

        
        # Make the request to archive the product
        response = requests.post(url, headers=headers, json={'query': mutation_string, 'variables': variables})

        if response.status_code != 200:
            message = f"Failed to archive product {product_id}. Status code: {response.status_code}, Response: {response.text}"
            print(message)
            return CustomResponse(data=message, status_code=response.status_code)

        # Check for any errors in the response
        response_json = response.json()
        
        if 'errors' in response_json:
            error_message = response_json['errors'][0]['message']
            print(f"Error received for product {product_id}: {error_message}")
            return CustomResponse(data=error_message, status_code=400)

        

    return CustomResponse(data="Products archived successfully", status_code=200)

def Shopify_bulk_unpublish_products(shop, access_token, api_version, product_ids, channel_id):
    """
    Bulk unpublish Shopify products.

    :param api_url: The Shopify GraphQL API URL, e.g., 'https://your-shop.myshopify.com/admin/api/2022-01/graphql.json'
    :param headers: Dictionary containing headers with API credentials, e.g., {'Content-Type': 'application/json', 'X-Shopify-Access-Token': 'your-access-token'}
    :param product_ids: List of product IDs to unpublish.
    """

    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token,
    }

    # STEP 1: CREATE THE JSONL FILE
    # Create and write to the JSONL file
    file_path='unpublish_products.jsonl'
    with open(file_path, 'w') as file:
        for product_id in product_ids:
            # Construct the line as a dictionary
            line_dict = {
                "id": f"{product_id}",
                "input": {
                    "publicationId": channel_id
                }
            }
            # Convert the dictionary to a JSON string and write it to the file
            json_line = json.dumps(line_dict)
            file.write(f"{json_line}\n")

    print(f"JSONL file created and saved to {file_path}")

    # STEP 2: UPLOAD JSONL FILE TO SHOPIFY
    print(f"Uploading JSONL file")
    # GraphQL mutation for stagedUploadsCreate
    mutation = '''
    mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
    stagedUploadsCreate(input: $input) {
        stagedTargets {
        url
        resourceUrl
        parameters {
            name
            value
        }
        }
        userErrors {
        field
        message
        }
    }
    }
    '''

    # Define the input for the mutation
    variables = {
        "input": [
            {
                "filename": "unpublish_products.jsonl",
                "mimeType": "text/jsonl", #"text/jsonl" "application/jsonl"
                "httpMethod": "POST",
                "resource": "BULK_MUTATION_VARIABLES" #FILE  BULK_MUTATION_VARIABLES
            }
        ]
    }

    # Send the request to create a staged upload
    print(f"Creating staged upload...")
    response = requests.post(url, headers=headers, data=json.dumps({'query': mutation, 'variables': variables}))

    # Check response
    if response.status_code != 200:
        message="Failed to create staged upload."
        print(message)
        return CustomResponse(data=message, status_code=response.status_code)

    response_json=response.json()
    if 'errors' in response_json:
        error_message = response_json['errors'][0]['message']
        # Log the error message or handle it as needed
        message=f"Error received: {error_message}"
        print(message)
        return CustomResponse(data=message, status_code=400)

    print(f"Staged upload created.")

    upload_url = response_json['data']['stagedUploadsCreate']['stagedTargets'][0]['url']
    upload_parameters = response_json['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters']
    resource_url = response_json['data']['stagedUploadsCreate']['stagedTargets'][0]['resourceUrl']

    #print(f"Upload url: {upload_url}")
    #print(f"Upload parameters: {upload_parameters}")
    #print(f"Resource url: {resource_url}")

    print(f"Uploading JSONL...")
    multipart_data = MultipartEncoder(
        fields={**{param['name']: param['value'] for param in upload_parameters}, 'file': ('unpublish_products.jsonl', open(file_path, 'rb'), 'text/jsonl')}
    )

    #print(f"Multipart data: {multipart_data}")

    response = requests.post(upload_url, data=multipart_data, headers={'Content-Type': multipart_data.content_type})
    if response.status_code not in [200, 201]:
        message=f"Failed to upload file. Status Code: {response.status_code} Response: {response.text}"
        print(message)
        return CustomResponse(data=message, status_code=response.status_code)

    print("File uploaded successfully.")

    key_value = next((param['value'] for param in upload_parameters if param['name'] == 'key'), None)
    staged_upload_path=key_value

    # STEP 3: EXECUTE THE MUTATION

    mutation_string = """
        mutation publishableUnpublish($id: ID!, $input: [PublicationInput!]!) {
        publishableUnpublish(id: $id, input: $input) {
            userErrors {
            field
            message
            }
        }
        }
        """

    # The GraphQL mutation for running a bulk operation
    bulk_mutation = '''
    mutation bulkOperationRunMutation($mutation: String!, $stagedUploadPath: String!) {
    bulkOperationRunMutation(mutation: $mutation, stagedUploadPath: $stagedUploadPath) {
        bulkOperation {
            id
            status
        }
            userErrors {
                field
                message
        }
    }
    }
    '''  

    # Variables to be passed with the mutation
    variables = {
        "mutation": mutation_string,
        "stagedUploadPath": staged_upload_path
    }

    # Make the GraphQL POST request to start the bulk operation
    response = requests.post(url, headers=headers, json={'query': bulk_mutation, 'variables': variables})

    # Inspect the response
    if response.status_code != 200:
        message=f"Failed to initiate bulk operation. {response.text}. Status code: {response.status_code}"
        print(message)
        return CustomResponse(data=message, status_code=response.status_code)    

    print("Bulk operation initiated successfully.")
     
    ## STEP 4 WAIT UNTIL FINISHED
    query = '''
    {
    currentBulkOperation {
        id
        status
        errorCode
        completedAt
    }
    }
    '''

    while True:
        response = requests.post(url, headers=headers, json={'query': query})
        if response.status_code == 200:
            response_json = response.json()
            if response_json['data']['currentBulkOperation']:
                current_status = response_json['data']['currentBulkOperation']['status']
                if current_status == 'COMPLETED':
                    print("Bulk operation completed.")
                    break
                elif current_status == 'RUNNING':
                    print("Bulk operation is still running. Checking again in 10 seconds...")
                    time.sleep(5)
                else:
                    print(f"Bulk operation status: {current_status}")
                    break
            else:
                print("No current bulk operation found. It might have not started properly or already finished.")
                break
        else:
            print(f"Failed to query bulk operation status. Status code: {response.status_code}")
            break
    
    return CustomResponse(data="OK", status_code=200)

def Shopify_process_handle(input_string):
    '''
    Returns handle in allowed shopify format
    '''
    # Convert ASCII control codes 0 to 32 and specific symbols to spaces to later convert them to hyphens
    for i in range(33):
        input_string = input_string.replace(chr(i), ' ')
    input_string = re.sub(r'[!#$%&*+,./:;<=>?@\\^`{|}~]', ' ', input_string)
    
    # Remove disallowed characters
    input_string = re.sub(r'["\'()\[\]]', '', input_string)
    
    # Convert spaces and hyphens to a placeholder, and then convert that placeholder to hyphens
    input_string = re.sub(r'[\s-]+', '-', input_string)
    
    # Remove placeholder hyphens at the end of the string
    input_string = re.sub(r'-+$', '', input_string)
    
    # Convert uppercase letters to lowercase
    input_string = input_string.lower()
    
    return input_string

def Shopify_execute_bulk_mutation(shop="", access_token="", api_version="", mutation="", staged_upload_path=""):
    
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    variables = {
        "mutation": mutation,
        "stagedUploadPath": staged_upload_path
    }

    # Make the GraphQL POST request to start the bulk operation
    response = requests.post(url, headers=headers, json={'query': mutationbulkOperationRunMutation, 'variables': variables})

    # Inspect the response
    if response.status_code != 200:
        message=f"Failed to initiate bulk operation. {response.text}. Status code: {response.status_code}"
        print(message)
        return CustomResponse(data=message, status_code=response.status_code)    

    print("Bulk operation initiated successfully.")
    print(response.text)
    
    ## STEP 4 WAIT UNTIL FINISHED
    query = '''
    {
    currentBulkOperation(type: MUTATION) {
        id
        status
        errorCode
        completedAt
    }
    }
    '''

    while True:
        response = requests.post(url, headers=headers, json={'query': query})
        
        if response.status_code == 200:
            response_json = response.json()
            if response_json['data']['currentBulkOperation']:
                current_status = response_json['data']['currentBulkOperation']['status']
                if current_status == 'COMPLETED':
                    print("Bulk operation completed.")
                    break
                elif current_status == 'RUNNING':
                    print("Bulk operation is still running. Checking again in 10 seconds...")
                    time.sleep(10)
                else:
                    print(f"Bulk operation status: {current_status}")
                    break
            else:
                print("No current bulk operation found. It might have not started properly or already finished.")
                break
        else:
            print(f"Failed to query bulk operation status. Status code: {response.status_code}")
            break

    print(response.text)
    return CustomResponse(data=response.text, status_code=response.status_code)  

def Shopify_upload_jsonl(shop="", access_token="", api_version="", file_path=""):
    # GraphQL mutation for stagedUploadsCreate
    mutation=mutationstagedUploadsCreate

    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    # Define the input for the mutation
    variables = {
        "input": [
            {
                "filename": file_path,
                "mimeType": "text/jsonl", 
                "httpMethod": "POST",
                "resource": "BULK_MUTATION_VARIABLES" 
            }
        ]
    }

    # Send the request to create a staged upload
    print(f"Creating staged upload...")
    response = requests.post(url, headers=headers, data=json.dumps({'query': mutation, 'variables': variables}))
    
    # Check response
    if response.status_code != 200:
        message="Failed to create staged upload."
        print(message)
        return CustomResponse(data=message, status_code=response.status_code)

    response_json=response.json()
    if 'errors' in response_json:
        error_message = response_json['errors'][0]['message']
        # Log the error message or handle it as needed
        message=f"Error received: {error_message}"
        print(message)
        return CustomResponse(data=message, status_code=400)

    print(f"Staged upload created.")

    upload_url = response_json['data']['stagedUploadsCreate']['stagedTargets'][0]['url']
    upload_parameters = response_json['data']['stagedUploadsCreate']['stagedTargets'][0]['parameters']
    resource_url = response_json['data']['stagedUploadsCreate']['stagedTargets'][0]['resourceUrl']

    print(f"Uploading JSONL...")
    multipart_data = MultipartEncoder(
        fields={**{param['name']: param['value'] for param in upload_parameters}, 'file': (file_path, open(file_path, 'rb'), 'text/jsonl')}
    )

    response = requests.post(upload_url, data=multipart_data, headers={'Content-Type': multipart_data.content_type})
    if response.status_code not in [200, 201]:
        message=f"Failed to upload file. Status Code: {response.status_code} Response: {response.text}"
        print(message)
        return CustomResponse(data=message, status_code=response.status_code)

    print("File uploaded successfully.")

    key_value = next((param['value'] for param in upload_parameters if param['name'] == 'key'), None)
    staged_upload_path=key_value

    return CustomResponse(data=staged_upload_path, status_code=200)

def Shopify_bulk_update_products(shop="", access_token="", api_version="", file_path="", mutation=""):
    """
    Bulk update Shopify products

    """

    # UPLOAD JSONL FILE TO SHOPIFY
    print(f"Uploading JSONL file")
    custom_response=Shopify_upload_jsonl(shop=shop, access_token=access_token, api_version=api_version, file_path=file_path)
    if custom_response.status_code!=200:
        message=f"Error creating staged response: {custom_response.data}"
        print(message)
        return CustomResponse(data=message, status_code=400)
    staged_upload_path = custom_response.data
    print(f"Staged upload path: {staged_upload_path}")

    # EXECUTE THE MUTATION
    print(f"Executing mutation staged_upload_path...")
    # print(f"Executing mutation {mutation} from path {staged_upload_path}...")
    custom_response=Shopify_execute_bulk_mutation(shop=shop, access_token=access_token, api_version=api_version, mutation=mutation, staged_upload_path=staged_upload_path)
    
    return CustomResponse(data=custom_response.data, status_code=custom_response.status_code)

def Shopify_get_image_url_from_gid(shop, access_token, api_version, gid, retries=3, delay=2):
    """
    Queries Shopify to get the public URL of an image using its GID.
    """
    query = '''
    query getImageUrl($id: ID!) {
      node(id: $id) {
        ... on MediaImage {
          image {
            url
          }
        }
      }
    }
    '''
    
    variables = {
        "id": gid
    }
    
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }
    
    for attempt in range(retries):
        response = requests.post(url, headers=headers, json={"query": query, "variables": variables})
        
        if response.status_code == 200:
            response_data = response.json()
            print(f"Response JSON (Attempt {attempt+1}): {response_data}")  # Log the response JSON for debugging
            
            if 'errors' in response_data:
                print(f"GraphQL errors: {response_data['errors']}")
                return None
            
            node = response_data.get('data', {}).get('node', None)
            
            if node and node.get('image') and node['image']['url']:
                return node['image']['url']
            else:
                print("Image URL not found in the response. Retrying...")
                time.sleep(delay)  # Wait before retrying
        else:
            print(f"Error fetching image URL: {response.status_code} - {response.text}")
            return None
    
    print("Exceeded maximum retries. Image URL not found.")
    return None

def Shopify_get_image_url_from_gid_OLD(shop, access_token, api_version, gid):
    """
    Queries Shopify to get the public URL of an image using its GID.
    """
    query = '''
    query getImageUrl($id: ID!) {
      node(id: $id) {
        ... on MediaImage {
          image {
            url
          }
        }
      }
    }
    '''
    
    variables = {
        "id": gid
    }
    
    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, headers=headers, json={"query": query, "variables": variables})
    print(f"response.json() {response.json()}")
    if response.status_code == 200:
        response_data = response.json()
        if 'errors' in response_data:
            print(f"GraphQL errors: {response_data['errors']}")
            return None
        node = response_data.get('data', {}).get('node', {})
        if node and 'image' in node:
            print(f"node {node}")
            image_url = node['image']['url']
            return image_url
        else:
            print("Image URL not found in the response.")
            return None
    else:
        print(f"Error fetching image URL: {response.status_code} - {response.text}")
        return None

###################### SPECIFIC FUNCTIONS
    
def Shopify_get_marketing_customer_list(shop="", access_token="", api_version="2024-01"):
    ''' Returns a dictionary with 2 lists, customer who are subscribe to email marketing and cutomers subscribed to SMS marketing'''
    # Assume Shopify_get_customers is defined elsewhere and correctly returns customer data
    response = Shopify_get_customers(shop, access_token, api_version)
    
    # Initialize dictionaries to hold subscribers
    marketing_lists = {
        'newsletter_subscribers': [],
        'sms_marketing_subscribers': []
    }
    
    # Proceed only if the response was successful
    if response.status_code != 200:
        print("Failed to retrieve customers. Status Code:", response.status_code)
        return response

    # Iterate through the customer data
    for customer in response.data:
        email_marketing_consent = customer.get('email_marketing_consent')
        if email_marketing_consent and email_marketing_consent.get('state') == 'subscribed':
            marketing_lists['newsletter_subscribers'].append({
                'first_name': customer.get('first_name', ''),
                'last_name': customer.get('last_name', ''),
                'email': customer.get('email', '')
            })
        
        sms_marketing_consent = customer.get('sms_marketing_consent')
        # Adjusted to check if sms_marketing_consent is not None and then proceed
        if sms_marketing_consent and sms_marketing_consent.get('state') == 'subscribed':
            marketing_lists['sms_marketing_subscribers'].append({
                'first_name': customer.get('first_name', ''),
                'last_name': customer.get('last_name', ''),
                'email': customer.get('email', '')  # Assuming you want the email for SMS subscribers
            })
    
    return CustomResponse(data=marketing_lists, status_code=200)
    
def Shopify_set_stock_zero_metafield_unpublish(shop="", access_token="", api_version="2024-01", metafield_key="custom.unpublish_after", filter_date="", reason="correction", reference_document_uri=""):
    '''
    Set stock to zero for all products with custom.unpublish_after 
    less than the in the filter_date
    '''
    # GET PRODUCTS AND RELATED INVENTORY ID WITH METAFIELD VALUE. 
    # Has to get all products in store in batches of 250, takes 3 seconds per batch
    custom_response = Shopify_get_products_and_inventoryid_with_metafields(shop=shop, access_token=access_token, api_version="2024-01", metafield_key=metafield_key, filterdate=filter_date)
    if custom_response.status_code != 200:
        error_message = "Error getting product with metafield value"
        print(error_message)
        return CustomResponse(data=error_message, status_code=400)

    filtered_products = custom_response.data
    
    # GET INVENTORY ITEMS FOR ALL VARIANTS
    inventory_item_ids = [item_id for product in filtered_products for item_id in product['variant_inventory_item_ids']]
    # GET INVENTORY LOCATION
    custom_response = Shopify_get_locations(shop=shop, access_token=access_token, api_version="2024-01")
    if custom_response.status_code!=200:
        error_message = "Error getting locations"
        print(error_message)
        return CustomResponse(data=error_message, status_code=400)
    locations = custom_response.data
    if locations:
        # Take the first location from the list
        first_location = locations[0]  # Access the first item in the list
        location_id = first_location['id']
        location_id = f"gid://shopify/Location/{location_id}"
    else:
        error_message = "Error No locations found."
        print(error_message)
        return CustomResponse(data=error_message, status_code=400)
    
    # SET STOCK TO ZERO. VERY FAST, JUST 1 MUTATION WITH ALL INVENTORY ITEMS
    reference_document_uri = ""
    custom_response=Shopify_set_inventory_to_zero(shop=shop, access_token=access_token, api_version="2024-01", inventory_item_ids=inventory_item_ids, location_id=location_id, reason=reason, reference_document_uri=reference_document_uri)
    if custom_response.status_code != 200:
        error_message = "Error setting inventory to zero"
        print(error_message)
        return CustomResponse(data=error_message, status_code=400)
    
    return CustomResponse(data="All OK", status_code=200)

def Shopify_collection_unpublish(shop="", access_token="", api_version="2024-01", collection_id=""):
    
    # GET PRODUCTS IN COLLECTION   
    print(f"Collection id: {collection_id}") 
    custom_response = Shopify_get_products_in_collection(shop=shop, access_token=access_token, collection_id=collection_id)
    if custom_response.status_code!= 200:
        error_message="Couldn't get products from collection"
        print(error_message)
        return CustomResponse(data=error_message, status_code=400)
    products = custom_response.data
    print(f"Total products in collection {len(products)}")
    
    # Filter products already unpublished to speed up
    # Filter wasn't working,
    # products = [product for product in products if product['published_at'] is not None]
    print(f"Total unpublished products in collection {len(products)}")

    channel_id = Shopify_get_online_store_channel_id(shop=shop, access_token=access_token, api_version=api_version) 
    
    # Bulk unpublish
    product_ids = [product['admin_graphql_api_id'] for product in products]
    custom_response=Shopify_bulk_unpublish_products(shop=shop, access_token=access_token, api_version=api_version, product_ids=product_ids, channel_id=channel_id)
    if custom_response!=200:
        return CustomResponse(data=custom_response.data, status_code=custom_response.status_code)
    
    message=f"Collection {collection_id}: {len(products)} products unpublished successfully."
    return CustomResponse(data=message, status_code=200)

def Shopify_collection_archive(shop="", access_token="", api_version="2024-01", collection_id=""):
    """
    Archives all products in the specified Shopify collection.

    :param shop: The Shopify store's domain.
    :param access_token: The Shopify API access token.
    :param api_version: The Shopify API version to use.
    :param collection_id: The ID of the collection whose products will be archived.
    """

    # STEP 1: Get products in the collection
    print(f"Fetching products for collection id: {collection_id}")
    # Only get active products to optimize
    custom_response = Shopify_get_products_in_collection(shop=shop, access_token=access_token, collection_id=collection_id)
    
    if custom_response.status_code != 200:
        error_message = "Couldn't get products from collection"
        print(error_message)
        return CustomResponse(data=error_message, status_code=400)
    
    products = custom_response.data
    print(f"Total products in collection: {len(products)}")
    
    # STEP 2: Filter products by their status (e.g., 'active', 'archived', or 'draft')
    active_products = [product for product in products if product.get('status') == 'active']    

    # Extract product IDs
    product_ids = [product['admin_graphql_api_id'] for product in active_products]
    
    if len(product_ids) == 0:
        message = f"No products found in collection {collection_id} to archive."
        print(message)
        return CustomResponse(data=message, status_code=200)
    message = f"Found {len(product_ids)} products in collection {collection_id} to archive."
    print(message)

    # STEP 2: Call Shopify_archive_products to archive the products
    print(f"Archiving products in collection {collection_id}...")
    custom_response = Shopify_archive_products(shop=shop, access_token=access_token, api_version=api_version, product_ids=product_ids)
    
    if custom_response.status_code != 200:
        error_message = "Failed to archive products in collection"
        print(error_message)
        return custom_response
    
    message = f"Collection {collection_id}: {len(product_ids)} products archived successfully."
    print(message)
    return CustomResponse(data=message, status_code=200)

def Shopify_publish_blog_post(shop="", access_token="", api_version="2024-01", blog_id="", title="", content="", author="", tags=[], published_at=None, image_path=None, image_url=None):
    """
    Publishes a blog post to Shopify.

    :param shop: The Shopify shop domain.
    :param access_token: The Shopify access token.
    :param api_version: The Shopify API version.
    :param blog_id: The ID of the blog to which the post will be published.
    :param title: The title of the blog post.
    :param content: The content of the blog post.
    :param author: The author of the blog post.
    :param tags: A list of tags for the blog post.
    :param published_at: The datetime when the blog post should be published.
    :param image_path: The local path to the image to be included in the blog post.
    :return: A CustomResponse object with the API response.

    example 
    blog_id="111726756151"
    title="test"
    content="<p>Posting right now.</p>"
    author="vinzo"
    tags=["tag1", "tag2"]
    image_path = "./test_image.png"
    image_url = 'https://getaiir.s3.eu-central-1.amazonaws.com/vinzo/banner/20240815174425_e7641ad0.png'
    """
    # Upload the image if provided
    if image_url is not None:
        print("--- Downloading file to local")
        image_path = download_file_local(url=image_url)

    if image_path:
        print("--- Uploading file to shopify")
        file_name = os.path.basename(image_path)
        upload_response = Shopify_upload_file(shop=shop, access_token=access_token, api_version=api_version, file_path=image_path, file_name=file_name, alt_text=title)  

        if upload_response.status_code == 200:
            gid = upload_response.data['data']['fileCreate']['files'][0]['id']
            upload_image_url = Shopify_get_image_url_from_gid(shop, access_token, api_version, gid)
            if not upload_image_url:
                print("Failed to retrieve image URL.")
                return CustomResponse(data="Failed to retrieve image URL", status_code=422)           
        else:
            print(f"Image upload failed: {upload_response.data}")
            return upload_response
    
        delete_local_file(file_name=image_path)

    url = f"https://{shop}.myshopify.com/admin/api/{api_version}/blogs/{blog_id}/articles.json"
    headers = {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

    data = {
        "article": {
            "title": title,
            "body_html": content,
            "author": author,
            "tags": ", ".join(tags),
        }
    }

    if published_at:
        data["article"]["published_at"] = published_at.isoformat()

    # Add the cover image if provided and successfully uploaded
    if upload_image_url:
        data["article"]["image"] = {
            "src": upload_image_url
        }

    response = requests.post(url, json=data, headers=headers)
    
    if response.status_code == 201:
        return CustomResponse(data=response.json(), status_code=200)
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return CustomResponse(data=response.text, status_code=response.status_code)

def Shopify_upload_file(shop, access_token, api_version="2024-01", file_path="", file_name="", alt_text=""):
    """
    Uploads a file to Shopify.

    :param shop: The Shopify shop domain.
    :param access_token: The Shopify access token.
    :param api_version: The Shopify API version.
    :param file_path: The local path to the file to be uploaded.
    :param file_name: The name of the file to be uploaded.
    :param alt_text: The alt text for the file.
    :return: A CustomResponse object with the API response.
    """
    try:
        # Read the file
        with open(file_path, 'rb') as file:
            file_data = file.read()
        file_size = os.path.getsize(file_path)

        file_extension = os.path.splitext(file_name)[1][1:].lower()
        mime_type = get_mime_type(file_extension)

        # Staged uploads query
        staged_uploads_query = '''
        mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
          stagedUploadsCreate(input: $input) {
            stagedTargets {
              resourceUrl
              url
              parameters {
                name
                value
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        '''

        staged_uploads_variables = {
            "input": [
                {
                    "filename": file_name,
                    "httpMethod": "POST",
                    "mimeType": mime_type,
                    "resource": "FILE",
                }
            ]
        }

        url = f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"
        headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json"
        }

        response = requests.post(
            url,
            headers=headers,
            data=json.dumps({
                "query": staged_uploads_query,
                "variables": staged_uploads_variables
            })
        )

        if response.status_code != 200:
            return CustomResponse(data=response.text, status_code=response.status_code)

        staged_uploads_query_result = response.json()

        # Check for errors
        if 'errors' in staged_uploads_query_result:
            return CustomResponse(data=staged_uploads_query_result['errors'], status_code=400)

        # Extract target info
        target = staged_uploads_query_result['data']['stagedUploadsCreate']['stagedTargets'][0]
        params = target['parameters']
        upload_url = target['url']
        resource_url = target['resourceUrl']

        # Prepare form data for AWS S3 upload
        form_data = {param['name']: param['value'] for param in params}
        form_data['file'] = (file_name, file_data, 'image/jpeg')

        # Upload to AWS S3
        upload_response = requests.post(
            upload_url,
            files=form_data
        )

        if upload_response.status_code not in [200, 201, 204]:
            return CustomResponse(data=upload_response.text, status_code=upload_response.status_code)

        create_file_query = '''
        mutation fileCreate($files: [FileCreateInput!]!) {
        fileCreate(files: $files) {
            files {
            alt
            id
            createdAt
            }
            userErrors {
            field
            message
            }
        }
        }
        '''

        create_file_variables = {
            "files": [
                {
                    "alt": alt_text,
                    "contentType": "IMAGE",
                    "originalSource": resource_url,
                }
            ]
        }

        response = requests.post(
            url,
            headers=headers,
            data=json.dumps({
                "query": create_file_query,
                "variables": create_file_variables
            })
        )

        if response.status_code != 200:
            return CustomResponse(data=response.text, status_code=response.status_code)

        return CustomResponse(data=response.json(), status_code=response.status_code)

    except Exception as e:
        return CustomResponse(data=str(e), status_code=500)

### FOR TEST PURPOSES
def main():

    load_dotenv()  # This loads the environment variables from .env
    shop_etw = os.getenv("SHOPIFY_ETW_SHOP")
    access_token_etw=os.getenv("SHOPIFY_ETW_TOKEN")
    collection_unpublish_etw=os.getenv("SHOPIFY_ETW_COLLECTION_UNPUBLISH")

    shop_vinzo = os.getenv("SHOPIFY_VINZO_SHOP")
    access_token_vinzo=os.getenv("SHOPIFY_VINZO_TOKEN")
    collection_unpublish_vinzo=os.getenv("SHOPIFY_VINZO_COLLECTION_UNPUBLISH")
    
    # Presenting the user with a list of options
    print("Options:")
    print("1. Shopify_get_metaobject_gid")
    print("2. Shopify_publish_blog_post")
    print("3. Shopify_upload_file")
    print("4. upload_file_to_shopify")
    print("5. Shopify_set_stock_zero_metafield_unpublish")
    print("6. Shopify_collection_archive")
    print("7. Shopify_collection_unpublish")  
    print("8. Shopify_update_metaobject")
    print("9. Test verify_token")
    print("10. Test Shopify_get_products_query")
    
    # Prompting the user to choose an option
    option = input("Please enter the number corresponding to your choice: ")

    # Handling user's choice
    if option == '1':
        metaobject_handle="vinzo-collection-banner"
        metaobject_type="product_banner"
        access_token = access_token_vinzo
        shop = shop_vinzo
        custom_response = Shopify_get_metaobject_gid(shop=shop, access_token=access_token, metaobject_type=metaobject_type, handle=metaobject_handle)
        print(custom_response)
    if option == '2':   
        title="test"
        content="<p>Posting right now.</p>"
        author="vinzo"
        tags=["tag1", "tag2"]
        published_at=datetime.now()
        image_path = "./test_image.png"
        image_url = "https://getaiir.s3.eu-central-1.amazonaws.com/vinzo/banner/20240815174425_e7641ad0.png"
        #image_url = None
        image_path = ""
        #image_path = "./20240815174425_e7641ad0.png"
        access_token = access_token_vinzo
        shop = shop_vinzo
        api_version = "2024-01"
       
        blog_id="111726756151"
         
        custom_response = Shopify_publish_blog_post(shop=shop, access_token=access_token, blog_id=blog_id, title=title, content=content, author=author, tags=tags, published_at=published_at, image_path=image_path, image_url=image_url )
        print(custom_response)
    if option == '3':   
        # Example Usage
        access_token = access_token_vinzo
        shop = shop_vinzo
        api_version = "2024-01"
        
        asset_key = "assets/your-image.jpg"        
        file_path = "test_image.png"

        # Define the URL and headers for the GET request
        
        url = f"https://{shop}.myshopify.com/admin/api/{api_version}/files.json"
        headers = {
            "Accept": "application/json",
            "X-Shopify-Access-Token": access_token
        }

        # Make the GET request to the Shopify API
        response = requests.get(url, headers=headers)

        # Print the response status code and text for debugging
        print(response.status_code)
        print(response.text)

        
    if option == '4':   
       
        file_path = "./test_image.png"
        file_name = "test_image.png"
        alt_text = "alt-tag"

        #result = Shopify_upload_file(shop=shop, access_token=access_token, api_version="2024-01", file_path=file_path, file_name=file_name, alt_text=alt_text)
        #print(result.data)
        
        # result = Shopify_upload_file_revised(shop=shop, access_token=access_token, api_version="2024-01", file_path=file_path, file_name=file_name, alt_text=alt_text)
        # print(result.data)
    if option == '5':
        access_token = access_token_vinzo
        shop = shop_vinzo
        filter_date="2024-09-27"
        Shopify_set_stock_zero_metafield_unpublish(shop=shop, access_token=access_token, api_version="2024-01", metafield_key="custom.unpublish_after", filter_date=filter_date, reason="correction", reference_document_uri="")
    if option == '6':
        access_token = access_token_vinzo
        shop = shop_vinzo
        collection_id = collection_unpublish_vinzo
        Shopify_collection_archive(shop=shop, access_token=access_token, collection_id=collection_id)
    if option == '7':
        access_token = access_token_vinzo
        shop = shop_vinzo
        collection_id = collection_unpublish_vinzo
        Shopify_collection_unpublish(shop=shop, access_token=access_token, collection_id=collection_id)
    if option == '8':
        shop = shop_vinzo
        access_token = access_token_vinzo
        api_version = "2024-01"
        metaobject_gid = 'gid://shopify/Metaobject/33751171383'
        banner_url = 'https://getaiir.s3.eu-central-1.amazonaws.com/vinzo/banner/20241010105026_235bcd71.png'
        mobile_banner_url = 'https://getaiir.s3.eu-central-1.amazonaws.com/vinzo/banner/20241010104941_d9dee006.png'
        product_url = 'https://9d9853.myshopify.com/products/protos-gran-reserva-2015'
        metaobject_banner_number = 1
        banner_title = ""
        banner_subtitle = ""
        button_url = 'https://getaiir.s3.eu-central-1.amazonaws.com/vinzo/banner/20241010105026_235bcd71.png'
        button_text = ""
        custom_response = Shopify_update_metaobject(shop=shop, access_token=access_token, api_version=api_version, metaobject_gid=metaobject_gid, banner_url=banner_url, 
                    mobile_banner_url=mobile_banner_url, product_url=product_url, banner_title=banner_title, banner_subtitle=banner_subtitle, 
                    button_text=button_text, button_url=button_url)
        
        print(custom_response.data)
        print(custom_response.status_code)
    if option == '9':
        # Test verify_token with both valid and invalid tokens
        access_token = access_token_vinzo
        shop = f"{shop_vinzo}.myshopify.com"
        api_version = "2024-01"
        
        print("\nTesting valid token:")
        print(f"Testing with shop: {shop}")
        result = verify_token(shop=shop, access_token=access_token)
        print(f"Valid token test result: {result}")
        
        print("\nTesting invalid token:")
        result = verify_token(shop=shop, access_token="invalid_token_here")
        print(f"Invalid token test result: {result}")

        access_token = access_token_etw 
        shop = f"{shop_etw}.myshopify.com"
        api_version = "2024-01"
        
        print("\nTesting valid token:")
        print(f"Testing with shop: {shop}")
        result = verify_token(shop=shop, access_token=access_token)
        print(f"Valid token test result: {result}")
        
        print("\nTesting invalid token:")
        result = verify_token(shop=shop, access_token="invalid_token_here")
        print(f"Invalid token test result: {result}")

    if option == '10':
        # Test Shopify_get_products_query
        print("\nTesting Shopify_get_products_query")
        print("Choose store:")
        print("1. Vinzo")
        print("2. ETW")
        store_choice = input("Enter store number: ")
        
        if store_choice == '1':
            shop = shop_vinzo
            access_token = access_token_vinzo
        else:
            shop = shop_etw
            access_token = access_token_etw
            
        api_version = "2024-01"
        
        print(f"\nTesting with shop: {shop}")
        print("Fetching products...")
        result = Shopify_get_products_query(shop=shop, access_token=access_token, api_version=api_version)
        
        if result.status_code == 200:
            print(f"Success! Retrieved {len(result.data)} products")
            if len(result.data) > 0:
                print("\nFirst product sample:")
                print(json.dumps(result.data[0], indent=2))
        else:
            print(f"Error: {result.data}")



if __name__ == '__main__':
    main()