# API reference https://docs.leonardo.ai/reference/creategeneration
# models https://docs.leonardo.ai/docs/elements-and-model-compatibility

import requests
import os
import time
import uuid
import json
from dotenv import load_dotenv
from RikPy.commonfunctions import rfplogger, download_file_local_with_query_parameters, delete_local_file
from RikPy.customresponse import CustomResponse

model_id_default = "6bef9f1b-29cb-40c7-b9df-32b51c1f67d3" #Default model # Leonardo Creative
model_id_anime = "1aa0f478-51be-4efd-94e8-76bfc8f533af" #Anime pastel dream (Marie Angel default). Pastel anime styling. Use with PMv3 and the anime preset for incredible range. 
model_id_AlbedoBase_XL = "2590401b-a844-4b79-b0fa-8c44bb54eda0" #A great generalist model that tends towards more CG artistic outputs. By alebdobond
model_id_Leonardo_Vision_XL ="5c232a9e-9061-4777-980a-ddc8e65647c6" #A versatile model that excels at realism and photography. Better results with longer prompts.
model_id_Leonardo_Diffusion_XL= "1e60896f-3c26-4296-8ecc-53e2afecc132" #The next phase of the core Leonardo model. Stunning outputs, even with short prompts.
model_id_DreamShaper_v5 = "d2fb9cf9-7999-4ae5-8bfe-f0df2d32abf8" #A versatile model great at both photorealism and anime, includes noise offset training, 
model_id_Marie = "4602459c-315a-4044-9d84-99fe7898fb0f"

# Load environment variables
load_dotenv()
leonardo_key = os.getenv("LEONARDO_KEY")
if not leonardo_key:
    raise ValueError("LEONARDO_KEY environment variable not set.")

negative_prompt = "long neck, deformed, long cloth, long dress, dark skin, ugly hands, bad hands"

# Helper function to get image file extension
def get_image_extension(image_file_path):
    _, extension = os.path.splitext(image_file_path)
    return extension.replace(".", "").lower()  # Remove the dot and convert to lowercase

# Function to check generation status with retries and sleep
def check_generation_status(generation_id, max_retries=60, interval=5):
    url = f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {leonardo_key}"
    }
    
    for _ in range(max_retries):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            generation_status = response.json().get('generations_by_pk', {}).get('status')
            if generation_status == 'COMPLETE':
                print("Image generation completed.")
                return 'COMPLETE'
            elif generation_status == 'PENDING':
                print("Waiting for image generation to complete...")
                time.sleep(interval)
            else:
                print(f"Image generation failed or unknown status: {generation_status}")
                return None
        else:
            print(f"Failed to get generation status. Status code: {response.status_code}")
            return None
    
    print("Image generation timed out.")
    return None

def create_payload(model_id, prompt, height, width, number_images, image_id=None, strength=0.5, guidance_scale=7, photoReal=False, alchemy=False, contrastRatio=1, promptMagic=True, promptMagicStrength=0.4):
    
    # This function supports both prompt-based and image-to-image generation
    payload = {
        "height": height,
        "width": width,
        "modelId": model_id,
        "prompt": prompt,
        "num_images": number_images,
        "alchemy": alchemy,
        "contrastRatio": contrastRatio,
        "guidance_scale": guidance_scale,
        "photoReal": photoReal,
        "promptMagic": promptMagic,
        "promptMagicStrength": promptMagicStrength,
        "public": False
    }
    
    if image_id:
        # Add image-to-image specific fields
        payload["init_image_id"] = image_id
        payload["init_strength"] = strength  # Strength controls how much the input image influences the result
    
    return payload

def create_payload_OLD(model_id, prompt, height, width, number_images=1):
    return {
        "height": height,
        "width": width,
        "modelId": model_id,
        "prompt": prompt,
        "num_images": number_images,
        "alchemy": False,
        "contrastRatio": 1,
        "guidance_scale": 7,
        "photoReal": False,
        "presetStyle": "LEONARDO",
        "promptMagic": True,
        "promptMagicStrength": 0.4,
        "promptMagicVersion": "v2",
        "public": False
    }

# Upload an image to Leonardo and get the `image_id` for image-to-image generation
def Leonardo_upload_image(image_file_path):
    # Step 1: Get the image file extension dynamically
    image_extension = get_image_extension(image_file_path)
    
    # Step 2: Get presigned URL for uploading the image
    url = "https://cloud.leonardo.ai/api/rest/v1/init-image"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {leonardo_key}",
        "content-type": "application/json"
    }
    payload = {"extension": image_extension}  # Use the correct file extension

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        print(f"Failed to get presigned URL. Status code: {response.status_code}")
        return None

    upload_response = response.json()['uploadInitImage']
    fields = upload_response['fields']
    presigned_url = upload_response['url']
    image_id = upload_response['id']  # This will be used for the image-to-image generation
    
    # Ensure fields is a dictionary before passing it to 'data'
    if isinstance(fields, str):
        # If fields is mistakenly a string, parse it (assuming it's JSON formatted)
        try:
            fields = json.loads(fields)
        except json.JSONDecodeError:
            print(f"Error: 'fields' is not in JSON format: {fields}")
            return None

    # Step 3: Upload the image via presigned URL
    with open(image_file_path, 'rb') as image_file:
        files = {'file': image_file}
        response = requests.post(presigned_url, data=fields, files=files)
    
    if response.status_code == 204:
        print(f"Image upload successful. Image ID: {image_id}")
        return image_id
    else:
        print(f"Image upload failed. Status code: {response.status_code}")
        return None

def Leonardo_retrieve_image(generation_id):
    if generation_id is None:
        print("No generation ID provided.")
        return []

    url = f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}"

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {leonardo_key}"
    }

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        print("Image retrieval successful.")
        # Extract the array of image URLs and metadata
        image_array = response.json()
        image_urls=[]
        for image in image_array['generations_by_pk']['generated_images']:
            image_urls.append(image['url'])

        return image_urls  # Returns the full image array from the response
    else:
        print(f"Image retrieval failed. Status code: {response.status_code}")
        print("Response:", response.text)
        return []
    
def Leonardo_list_all_models():
 
    url = "https://cloud.leonardo.ai/api/rest/v1/platformModels"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {leonardo_key}"
    }
    response = requests.get(url, headers=headers)
    print(response.text)
    return response.text

def Leonardo_create_model_map():
    url = "https://cloud.leonardo.ai/api/rest/v1/platformModels"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {leonardo_key}"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        model_data = response.json()
        model_map = {}
        
        # Loop through each model and map the name to its id
        for model in model_data.get('custom_models', []):
            model_name = model['name']
            model_id = model['id']
            model_map[model_name] = model_id

        # Add manually specified models for backward compatibility
        model_map.update({
            "model_id_default": "6bef9f1b-29cb-40c7-b9df-32b51c1f67d3",  # Default model
            "model_id_anime": "1aa0f478-51be-4efd-94e8-76bfc8f533af",  # Anime pastel dream
            "model_id_AlbedoBase_XL": "2590401b-a844-4b79-b0fa-8c44bb54eda0",       # Generalist model
            "model_id_Leonardo_Vision_XL": "5c232a9e-9061-4777-980a-ddc8e65647c6",  # Realism/photography model
            "model_id_Leonardo_Diffusion_XL": "1e60896f-3c26-4296-8ecc-53e2afecc132", # Core Leonardo model
            "model_id_DreamShaper_v5": "d2fb9cf9-7999-4ae5-8bfe-f0df2d32abf8",      # Versatile model
            "model_id_Marie": "4602459c-315a-4044-9d84-99fe7898fb0f"                # Model for pastel/anime style
        })
        
        return model_map
    else:
        print(f"Failed to retrieve models. Status code: {response.status_code}")
        return {}

def Leonardo_generate_image(model_name, prompt, height, width, payload="", number_images=1, image_file_path=None, strength=0.5, guidance_scale=7, 
                            photoReal=False, alchemy=False, contrastRatio=1, promptMagic=True, promptMagicStrength=0.4, controlnets=None):
    # First, check if we are doing image-to-image generation or prompt-based generation
    image_id = None
    local_image_path = image_file_path
        
    # If image_file_path is a URL, download it locally
    if image_file_path and image_file_path.startswith("http"):
        random_filename = f"{uuid.uuid4()}.png"  # Generate a random filename
        print(f"Downloading image from URL: {image_file_path} as {random_filename}")
        local_image_path=download_file_local_with_query_parameters(file_url=image_file_path, generate_random_filename=random_filename)
        print(f"Image downloaded to {local_image_path}")

    if local_image_path and not local_image_path.startswith("http"):
        # If image_file_path is provided, upload the image and get the image ID
        image_id = Leonardo_upload_image(local_image_path)
        if not image_id:
            message="Image upload failed. Cannot proceed with image-to-image generation."
            print(message)
            return CustomResponse(data=message, status_code=400)
        
        # Optionally, delete the downloaded local file after uploading it
        if image_file_path and image_file_path.startswith("http"):
            delete_local_file(local_image_path)

    # Model map to dynamically choose model based on the input
    model_map=Leonardo_create_model_map()
    
    # Validate the model_name and map it to the model_id, or use the default model if not found
    engine_model_id = model_map.get(model_name, model_id_default)

    if model_name not in model_map:
        print(f"Model '{model_name}' not found, using default model '{model_id_default}'.")

    # Create the payload
    if not payload:
        payload = create_payload(engine_model_id, prompt, height, width, number_images, image_id, strength, guidance_scale, photoReal, alchemy, 
                                 contrastRatio, promptMagic, promptMagicStrength)
        
        # Add ControlNet configuration to the payload if available
        if controlnets:
            for controlnet in controlnets:
                # If 'initImageId' is not provided in the ControlNet config, use the uploaded 'image_id'
                if "initImageId" not in controlnet and image_id:
                    controlnet["initImageId"] = image_id
            payload["controlnets"] = controlnets

    url = "https://cloud.leonardo.ai/api/rest/v1/generations"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {leonardo_key}"
    }

    # Send image generation request
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        print("Image generation initiated.")
        response_data = response.json()
        generation_id = response_data.get('sdGenerationJob', {}).get('generationId')  # Adjust this according to the actual response structure
        if generation_id is None:
            message="Generation ID not found in response."
            print(message)
            return CustomResponse(data=message, status_code=400)

        # Check the status of the generation job periodically until it's completed
        status = check_generation_status(generation_id)
        if status == 'COMPLETE':
            return CustomResponse(data=generation_id, status_code=200)
        else:
            message=f"Image generation failed on generation check with generation ID {generation_id}."
            print(message)
            return CustomResponse(data=message, status_code=400)
    else:
        message = f"Image generation failed. Response: {response.text}. Status code: {response.status_code}"
        print(message)
        return CustomResponse(data=message, status_code=400)

def Leonardo_generate_image_OLD(model_id, prompt, height, width, payload=None, number_images=1):
    
    engine_model_id=model_id_default
    model_map = {
        "model_id_anime": model_id_anime,
        "model_id_AlbedoBase_XL": model_id_AlbedoBase_XL,
        "model_id_Leonardo_Vision_XL": model_id_Leonardo_Vision_XL,
        "model_id_Leonardo_Diffusion_XL": model_id_Leonardo_Diffusion_XL,
        "model_id_DreamShaper_v5": model_id_DreamShaper_v5,
        "model_id_Marie": model_id_Marie
    }
    engine_model_id = model_map.get(model_id, model_id_default)

    url = "https://cloud.leonardo.ai/api/rest/v1/generations"

    if not payload:
        payload = create_payload(engine_model_id, prompt, height, width, number_images)

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {leonardo_key}"
    }

    # Send image generation request
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        print("Image generation initiated.")
        response_data = response.json()
        generation_id = response_data.get('sdGenerationJob', {}).get('generationId')  # Adjust this according to the actual response structure
        if generation_id is None:
            print("Generation ID not found in response.")
            return None
        
        # Check the status of the generation job periodically until it's completed
        status = check_generation_status(generation_id)
        if status == 'COMPLETE':
            return generation_id
        else:
            return None
        
    else:
        print(f"Image generation failed. Status code: {response.status_code}")
        rfplogger(response.text)
        print("Response:", response.text)
        return None
    
### FOR TEST PURPOSES
def main():
    
    # Presenting the user with a list of options
    print("Options:")
    print("1. Test image to image")
    print("2. List all models")
    print("3. Create model map")
    
    # Prompting the user to choose an option
    option = input("Please enter the number corresponding to your choice: ")

    # Handling user's choice
    if option == '1':
        image_file_path = "https://getaiir.s3.eu-central-1.amazonaws.com/etw/image/20241020090857_6f656920.png"  # Provide the path to the image to upload (optional)
        prompt = "boxes of grapes in a vineyard"
        model_name = "Leonardo Vision XL"
        controlnets = [
            {
                # "initImageId": "uploaded_image_id_123",  # Replace with actual uploaded image ID if known
                "initImageType": "image",                # Specify the type of input, like "image" or "mask"
                "preprocessorId": 67,                    # Preprocessor ID for guidance, e.g., 67 for Style Reference
                "strengthType": "soft",                  # Apply soft or hard guidance
                "influence": 0.75                        # Control how strongly the input guides generation (0-1)
            }
        ]
        generation_id = Leonardo_generate_image(
            model_name=model_name,  # You can choose from the available models
            prompt=prompt,
            height=512,
            width=512,
            number_images=1,
            image_file_path=image_file_path,  # Provide image file path for image-to-image (leave None for prompt-based)
            strength=0.7,  # Adjust strength to control the influence of the uploaded image
            guidance_scale=8,  # Adjust guidance for how much it follows the prompt
            photoReal=False  # Example of enabling photorealism
        )

        # Retrieve generated images
        if generation_id:
            generated_images = Leonardo_retrieve_image(generation_id)
            print(generated_images)
    elif option == '2':
        Leonardo_list_all_models()
    elif option == '3':
        model_map=Leonardo_create_model_map()
        print(f"model map {model_map}")

if __name__ == "__main__":
    main()