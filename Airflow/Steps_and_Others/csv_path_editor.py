import json


def replace_path_in_json(file_path, new_path):
    try:
        # Load JSON from the existing file
        with open(file_path, 'r') as file:
            data = json.load(file)

        # Replace the value of "path" with the new path
        data["path"] = new_path

        # Save the updated JSON back to the file
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=2)

        print(f'Successfully replaced the value of "path" in JSON file "{file_path}".')

    except FileNotFoundError:
        print(f'JSON file "{file_path}" not found.')

    except Exception as e:
        print(f'An error occurred while replacing the value of "path" in JSON file "{file_path}": {e}')

