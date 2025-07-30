import json, os, yaml
from ruamel.yaml import YAML
from typing import Dict, List, Union, Tuple


class DbtProject:
    def __init__(self, manifest_path: str):
        """
        Initialize the DBT project object.

        Args:
            manifest_path (str): The path to the manifest.json file.
        """
        with open(manifest_path, "r") as file:
            self.manifest = json.load(file)
        self.models = self.get_ordered_models()
        self.documentation = {
            model_name.split(".")[-1]: {
                "name": model_name.split(".")[-1],
                "description": "",
                "columns": [],
            }
            for model_name in self.manifest["nodes"].keys()
            if self.manifest["nodes"][model_name]["resource_type"] == "model"
        }

    def get_ordered_models(self) -> List[str]:
        """
        Get the models in the order they should be processed.

        Returns:
            List[str]: A list of model names in the order they should be processed.
        """
        models = []
        added_models = set()

        # Find models that only read from sources
        for model_name, model in self.manifest["nodes"].items():
            if model["resource_type"] == "model" and all(
                parent.startswith("source.") for parent in model["depends_on"]["nodes"]
            ):
                models.append(model_name)
                added_models.add(model_name)
                print(f"Added model '{model_name}' to the list")

        # Process remaining models with dependencies
        while len(models) < len(self.manifest["nodes"]):
            added = False  # Track if any models were added in this iteration
            for model_name, model in self.manifest["nodes"].items():
                if model["resource_type"] == "model" and model_name not in added_models:
                    parent_models = model["depends_on"]["nodes"]
                    if all(
                        parent_model in added_models for parent_model in parent_models
                    ):
                        models.append(model_name)
                        added_models.add(model_name)
                        added = True  # Set added to True if a model is added
                        print(f"Added model '{model_name}' to the list")
            if not added:
                # If no models were added in this iteration, break the loop to avoid infinite loop
                break

        return models

    def get_model_info(
        self, model_name: str
    ) -> Dict[str, Union[str, List[str], Dict[str, str]]]:
        """
        Get the information for a specific model.

        Args:
            model_name (str): The name of the model to get information for.

        Returns:
            Dict[str, Union[str, List[str], Dict[str, str]]]: A dictionary containing the model's information.
        """
        model = self.manifest["nodes"][model_name]
        parent_descriptions = []
        existing_model_description = (
            model["description"] if "description" in model else None
        )
        existing_column_descriptions = {
            col_name: col["description"]
            for col_name, col in model["columns"].items()
            if "description" in col
        }

        if "depends_on" in model:
            parent_descriptions = [
                self.documentation[parent]["description"]
                for parent in model["depends_on"]["nodes"]
                if parent in self.documentation
                and self.documentation[parent]["description"].strip() != ""
            ]
        return {
            "name": model_name,
            "fields": list(model["columns"].keys()),
            "relationships": [
                f"{col_name} relates to {col['relation_to']}"
                for col_name, col in model["columns"].items()
                if "relation_to" in col
            ],
            "missing_column_descriptions": [
                (col_name, col, existing_column_descriptions.get(col_name, None))
                for col_name, col in model["columns"].items()
            ],
            "parent_descriptions": parent_descriptions,
            "raw_code": model.get("raw_sql", model.get("raw_code", "")),
            "existing_model_description": existing_model_description,
        }

    def update_description(
        self,
        model_name: str,
        description: str,
        column_descriptions: List[Tuple[str, str]],
    ):
        """
        Update the description and column descriptions for a model.

        Args:
            model_name (str): The name of the model to update.
            description (str): The new description for the model.
            column_descriptions (List[Tuple[str, str]]): A list of tuples containing column names and their new descriptions.
        """
        model_name_clean = model_name.replace("model.warehouse.", "")
        if model_name_clean in self.documentation:
            self.documentation[model_name_clean]["description"] = description
            self.documentation[model_name_clean][
                "columns"
            ].clear()  # Clear existing columns
            for col_info in column_descriptions:
                col_name, col_desc = col_info
                column = {"name": col_name, "description": col_desc}
                self.documentation[model_name_clean]["columns"].append(column)

    def get_documentation(
        self,
    ) -> Dict[str, Dict[str, Union[str, List[Dict[str, str]]]]]:
        """
        Get the documentation for all models.

        Returns:
            Dict[str, Dict[str, Union[str, List[Dict[str, str]]]]]: A dictionary containing the documentation for all models.
        """
        return self.documentation

    def save_yaml(self, dir_path: str, model_name: str):
        """
        Save the documentation for a model to a YAML file.

        Args:
            dir_path (str): The directory path where the YAML file should be saved.
            model_name (str): The name of the model to save documentation for.
        """
        model_name_clean = model_name.replace("model.warehouse.", "")

        yaml = YAML()
        yaml.indent(sequence=4, offset=2)

        # Get the original_file_path from the manifest and replace .sql with .yml
        original_file_path = self.manifest["nodes"][model_name][
            "original_file_path"
        ].replace(".sql", ".yml")
        # Join the provided directory path and the original_file_path to get the correct file path
        file_path = os.path.join(dir_path, original_file_path)

        print(
            f"Saving documentation for model '{model_name_clean}' to file: {file_path}"
        )

        if os.path.exists(file_path):
            with open(file_path, "r") as file:
                data = yaml.load(file)

            if not data.get("models"):
                data["models"] = []

            for model in data["models"]:
                if model["name"] == model_name_clean:
                    # If there's no description at the model level, generate one
                    if not model.get("description"):
                        model["description"] = self.documentation[model_name_clean][
                            "description"
                        ]

                    # If there's no description at the column level, generate one
                    if model.get("columns"):
                        for column in model["columns"]:
                            for doc_column in self.documentation[model_name_clean][
                                "columns"
                            ]:
                                if column["name"] == doc_column["name"] and (
                                    not column.get("description")
                                    or column.get("description") == ""
                                ):
                                    column["description"] = doc_column["description"]
                    break
            else:
                # If the model doesn't exist in the file, add it
                data["models"].append(self.documentation[model_name_clean])

            with open(file_path, "w") as file:
                yaml.dump(data, file)
        else:
            print(f"File doesn't exist: {file_path}")


        print(
            f"Updated documentation for model '{model_name_clean}' in file: {file_path}"
        )
        
