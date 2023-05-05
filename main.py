from etl_transform import template_etl
import logging
import json

# ---------------
# Games ETL class
# ---------------
class games_etl(template_etl):
    """Games ETL class

       Example of valid initial template:
        {
            "The Legend Of Zelda": {
                "The Minish Cap": {
                    "platform": "Gameboy Advance"
                }
            }
        }
    """

    def transform_yakuza_01_add_games(self):
        """Add the Yakuza games to the template"""
        # Example of auto recursive update
        self.recursive_update({
            "Yakuza": {
                "Yakuza Kiwami": {
                    "platform": "Playstation 4"
                },
                "Yakuza Kiwami 2": {
                    "platform": "Playstation 4"
                }
            }
        })

    def transform_yakuza_02_fix_game_platform(self):
        """Fix the platform of the Yakuza game

           Update Playstation 4 to Playstation 3
        """
        # Example of auto recursive update
        self.recursive_update(
            {
                "Yakuza": {
                    "Yakuza Kiwami": {
                        "platform": "Playstation 3"
                    },
                    "Yakuza Kiwami 2": {
                        "platform": "Playstation 3"
                    }
                }
            }
        )

    def transform_yakuza_03_delete_kiwami(self):
        """Delete the Yakuza Kiwami game"""
        # Example of manual transformation
        template = self.get_template()
        del template["Yakuza"]["Yakuza Kiwami"]
        self.set_template(template)



# ----
# Main
# ----
if __name__ == "__main__":
    # Create a template
    with open("./template_example.json", "r") as f:
        template = json.load(f)

    # Create a template_etl object
    template_etl = games_etl(template, logging.DEBUG)

    # Process the template
    template_etl.process(template)

    # Print the template as JSON
    print(json.dumps(template_etl.get_template(), indent=4))
