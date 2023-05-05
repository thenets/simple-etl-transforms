import sys
import json

import logging
logging.basicConfig(level=logging.DEBUG)

# ------------------
# Template ETL class
# ------------------
class template_etl:
    # The constructor receives a dictionary as the template
    def __init__(self, template:dict, log_level=logging.INFO):
        """Constructor for template_etl

           Parameters:
            template (dict): The template to be used
            log_level (int): The logging level to be used
        """
        # Set the template
        self.__template = template
        self.__template_initial = template

        # Setup logging
        self.logger = logging.getLogger("etl")
        self.logger.propagate = False
        # Always log to stderr
        handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter("\033[36m%(asctime)s - %(name)s - %(levelname)s - %(message)s\033[0m")
        handler.setFormatter(formatter)
        handler.setLevel(log_level)
        self.logger.addHandler(handler)

        # Debug logging
        self.logger.debug(f"New template: \n{json.dumps(self.__template, indent=4)}")

    def get_template(self) -> dict:
        """Return the template"""
        return self.__template

    def set_template(self, template:dict):
        """Set the template"""

        # Log debug, the current template
        self.logger.debug(f"New template: \n{json.dumps(self.__template, indent=4)}")

        self.__template = template

    def recursive_update(self, new_template):
        """Recursively update the template with the new template"""
        d = self.get_template()
        u = new_template

        def recursive_update(d, u):
            for k, v in u.items():
                if isinstance(v, list) or isinstance(v, dict):
                    d[k] = recursive_update(d.get(k, {}), v)
                else:
                    d[k] = v
            return d

        recursive_update(d, u)

        self.logger.debug(f"New template: \n{json.dumps(self.__template, indent=4)}")

    def process(self, data:dict) -> dict:
        """Process the data using the template and return the result

           Search for all functions in the class starting with transform_ and call them sequentially.
        """
        # Reset the template
        self.__template = self.__template_initial

        # Get all functions in the class
        functions = [func for func in dir(self) if callable(getattr(self, func))]

        # Get all functions starting with transform_
        functions_transform = [func for func in functions if func.startswith("transform_")]

        # Log info, all the functions that will be called
        log_function_names = ", ".join(functions_transform)
        self.logger.info(f"Functions to be called: {log_function_names}")

        # Loop through all functions
        for func in functions_transform:
            # Log info, the function that will be called
            self.logger.info(f"Calling function: {func}")
            # Call the function
            getattr(self, func)()

        return self.get_template()
