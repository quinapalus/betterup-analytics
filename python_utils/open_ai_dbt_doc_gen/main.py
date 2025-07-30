import openai, os
from dbt_project import DbtProject  # This is your custom class to handle DBT project
from dotenv import load_dotenv

# Load the .env file
load_dotenv()

# Set the OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")
DISABLE_API_CALLS = os.getenv("DISABLE_API_CALLS", False)

# OS Path to the manifest.json file
manifest_path = os.path.join(
    os.getcwd(), os.getenv("DBT_MANIFEST_PATH", "./manifest.json")
)


def generate_description(model_info, existing_description=None):
    model_name_clean = model_info['name'].replace("model.warehouse.", "")
    if existing_description:
        print(
            f"Using existing description for model '{model_info['name']}': {existing_description}"
        )
        return existing_description
    prompt = f"Generate a concise, informative, helpful description for the DBT model {model_name_clean}. This is additional context information for making a better description, don't reference it directly: This model has fields {', '.join(model_info['fields'])}. It has the following relationships: {', '.join(model_info['relationships'])}. The parent models have the following descriptions: {', '.join(model_info['parent_descriptions'])}. The raw SQL code for this model is: {model_info['raw_code']}\nEnd of SQL code. End of additional context information."

    if not DISABLE_API_CALLS:
        response = openai.Completion.create(
            engine="text-davinci-003", prompt=prompt, max_tokens=500
        )
        return response.choices[0].text.strip() + ' [GenAI Description]'
    return ""


def generate_column_description(column_info, raw_code, existing_description=None):
    if existing_description:
        print(
            f"Using existing description for column '{column_info[0]}' in model '{column_info[1]}': {existing_description}"
        )
        return existing_description
    prompt = f"Generate a concise description for the column '{column_info[0]}' in the DBT model '{column_info[1]}'. The raw SQL code for this model is:\n{raw_code}\nEnd of SQL code. Do not include the model name, and keep the description to one or two sentences."

    if not DISABLE_API_CALLS:
        response = openai.Completion.create(
            engine="text-davinci-003", prompt=prompt, max_tokens=200
        )
        return response.choices[0].text.strip() + ' [GenAI Description]'
    return ""


def main():
    project = DbtProject(manifest_path)
    ordered_models = project.models  # Use the ordered models

    generated_docs_directory = "generated_docs"
    if not os.path.exists(generated_docs_directory):
        os.makedirs(generated_docs_directory)

    print("Starting documentation generation process...\n")

    for current_model in ordered_models:
        current_model_info = project.get_model_info(current_model)

        # Skip models that already have descriptions
        if current_model_info is None:
            continue

        yaml_file_path = os.path.join(generated_docs_directory, f"{current_model}.yml")

        if os.path.exists(yaml_file_path):
            print(f"Skipping model '{current_model}', documentation already exists.")
            continue

        print(f"Processing model: {current_model}")
        model_info = project.get_model_info(current_model)
        print(f"Model info obtained for model '{current_model}' from manifest.json")

        description = generate_description(
            model_info, model_info["existing_model_description"]
        )
        column_descriptions = []
        print(f"Generating column descriptions for model '{current_model}'")
        for col_name, col, existing_description in model_info[
            "missing_column_descriptions"
        ]:
            column_description = generate_column_description(
                (col_name, model_info["name"]),
                model_info["raw_code"],
                existing_description,
            )
            column_descriptions.append((col_name, column_description))

        model_info = project.get_model_info(current_model)

        project.update_description(current_model, description, column_descriptions)
        print(f"Updated documentation for model '{current_model}'")

        # Save the generated documentation for the current model to a separate YAML file
        project.save_yaml("warehouse", current_model)

    print("\nDocumentation generation process completed!")


if __name__ == "__main__":
    main()

