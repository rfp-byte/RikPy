import openai
import os
import logging
from .customresponse import CustomResponse
import requests
import json
import tiktoken

def get_default_chat_model() -> str:
    """
    Returns the model your app should use when the caller
    doesn't override it.

    Order of precedence:
      1.  OPENAI_DEFAULT_CHAT_MODEL env-var
      2.  Hard-coded safe default ('gpt-4o')
    """
    return os.getenv("OPENAI_DEFAULT_CHAT_MODEL", "gpt-4o")

def get_default_image_model() -> str:
    """
    Returns the default image model to use.
    Order of precedence:
      1.  OPENAI_DEFAULT_IMAGE_MODEL env-var
      2.  Hard-coded fallback ('dall-e-3')
    """
    return os.getenv("OPENAI_DEFAULT_IMAGE_MODEL", "dall-e-3")

def OpenAI_generate_response(prompt, openai_key="", model: str | None = None):
    
    try:
        # Executes the prompt and returns the response without parsing
        logging.info("Warming Up the Wisdom Workshop!")
        openai.api_key = openai_key

        chosen_model = model or get_default_chat_model()
        logging.info(f"Assembling Words of Wisdom with {chosen_model}!")

        details_response = openai.chat.completions.create(
            model=chosen_model,
            messages=[
                {
                    "role": "user",
                    "content": prompt,  # Your prompt goes here
                }
            ]
        )
        
        return CustomResponse(data=details_response, status_code=200)
    
    ## https://platform.openai.com/docs/guides/error-codes/python-library-error-types
    except openai.InternalServerError as e:
        logging.error(f"OpenAI Internal Server Error: {str(e)}")
        return CustomResponse(data={"error": str(e)}, status_code=400)
    except openai.RateLimitError as e: 
        logging.error(f"OpenAI Rate Limit Error: {str(e)}")
        return CustomResponse(data={"error": str(e)}, status_code=429)
    except openai.BadRequestError as e:
        logging.error(f"OpenAI Bad Request Error: {str(e)}")
        return CustomResponse(data={"error": str(e)}, status_code=500)
    except openai.APIError as e:
        logging.error(f"OpenAI API Error: {str(e)}")
        return CustomResponse(data={"error": str(e)}, status_code=500)
    except Exception as e:
        logging.error(f"Unexpected error in OpenAI_generate_response: {str(e)}")
        return CustomResponse(data={"error": str(e)}, status_code=500)
        
def OpenAI_generate_image(image_prompt, number_images=1, quality="standard", size="1024x1024", 
        openai_key="", 
        model: str | None = None):
    
    try:
        # Executes the prompt and returns the response without parsing
        
        logging.info("Sparking the Synapses of Silicon!")
        openai.api_key = openai_key

        chosen_model = model or get_default_image_model()
        logging.info(f"Summoning Pixels from the Digital Depths with {chosen_model}!")
        logging.info(f"Image prompt: {image_prompt}")
        
        image_response = openai.images.generate(
            model=chosen_model,
            prompt=image_prompt,
            n=number_images,
            quality=quality,
            size=size
        )
        
        #return image_response
        return CustomResponse(data=image_response, status_code=200)
    
    ## https://platform.openai.com/docs/guides/error-codes/python-library-error-types
    except openai.InternalServerError as e:
        logging.error(f"OpenAI Internal Server Error: {str(e)}")
        return CustomResponse(data={"error": str(e)}, status_code=400)
    except openai.RateLimitError as e: 
        logging.error(f"OpenAI Rate Limit Error: {str(e)}")
        return CustomResponse(data={"error": str(e)}, status_code=429)
    except openai.BadRequestError as e:
        logging.error(f"OpenAI Bad Request Error: {str(e)}")
        return CustomResponse(data={"error": str(e)}, status_code=500)
    except openai.APIError as e:
        logging.error(f"OpenAI API Error: {str(e)}")
        return CustomResponse(data={"error": str(e)}, status_code=500)
    except Exception as e:
        logging.error(f"Unexpected error in OpenAI_generate_image: {str(e)}")
        return CustomResponse(data={"error": str(e)}, status_code=500)
    
def OpenAI_generate_image_request(image_prompt, number_images=1, quality="standard", size="1024x1024", 
        openai_key="", 
        model: str | None = None):
    
    try:
        chosen_model = model or get_default_image_model()
        logging.info(f"Summoning Pixels from the Digital Depths with {chosen_model}!")
        logging.info(f"Image prompt: {image_prompt}")

        url = "https://api.openai.com/v1/images/generations"

        headers = {
            "Authorization": f"Bearer {openai_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": chosen_model,
            "prompt": image_prompt,
            "n": number_images,
            "quality": quality,
            "size": size
        }      

        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response_json = response.json()

        # Log all headers for debugging
        logging.info(f"Response Headers: {response.headers}")

        if response.status_code != 200: 
            logging.error(f"OpenAI API request failed with status {response.status_code}: {response_json.get('error', 'Unknown error')}")
            return CustomResponse(data={"error": response_json.get("error", "Unknown error")}, status_code=response.status_code)
        # Extract token usage information from headers
        token_usage = response.headers.get('X-OpenAI-Usage')
        return CustomResponse(data={"response": response_json, "usage": token_usage}, status_code=200)

    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception in OpenAI_generate_image_request: {str(e)}")
        return CustomResponse(data={"error": str(e)}, status_code=500)

def OpenAI_num_tokens_from_string(string: str, model: str) -> int:
        """Returns the number of tokens in a text string."""
        encoding = tiktoken.encoding_for_model(model)
        num_tokens = len(encoding.encode(string))
        return num_tokens