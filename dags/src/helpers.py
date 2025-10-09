import logging

logger = logging.getLogger("helper")
def consolidate_configuration(configuration: dict, defaults: dict):
    """
    merge the configuration with the default value to consolidate the configuration
   and create the endpoint
    """
    for key in defaults: 
        logger.warning(key)
        if isinstance(configuration[key], list) and isinstance(defaults[key], list):
            configuration[key] = configuration[key] + defaults[key]
        if isinstance(defaults[key], dict) and isinstance(configuration[key], dict):
            logger.warning(defaults[key])
            defaults[key].update(configuration[key])
            configuration[key] = defaults[key]
        if key not in configuration:
            configuration[key] = defaults[key]

    logger.info("configuration %s", configuration)
    logger.info("defaults %s", defaults)
    return configuration



