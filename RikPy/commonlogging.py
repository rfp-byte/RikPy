import logging

def configure_logger():  
    # Configure the logger
    logging.basicConfig(level=logging.INFO)  # Set the logging level to INFO or your preferred level
    logger = logging.getLogger(__name__)  # Create a logger object for your module (__name__ will use the current module's name)
    return logger