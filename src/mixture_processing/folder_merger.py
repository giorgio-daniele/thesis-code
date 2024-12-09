import os
import shutil
import uuid
import argparse

def copy_and_rename_files(folder1: str, folder2: str, destination_folder: str) -> None:
    # Ensure destination folder exists
    os.makedirs(destination_folder, exist_ok=True)
    
    print(folder1, folder2)
    # return
    
    # Helper function to generate unique file names
    def generate_unique_filename() -> str:
        return str(uuid.uuid4())

    # Copy files from folder1 to destination with unique names
    for filename in os.listdir(folder1):
        source_file = os.path.join(folder1, filename)
        if os.path.isfile(source_file):
            unique_filename = generate_unique_filename() + os.path.splitext(filename)[1]
            destination_file = os.path.join(destination_folder, unique_filename)
            shutil.copy(source_file, destination_file)

    # Copy files from folder2 to destination with unique names
    for filename in os.listdir(folder2):
        source_file = os.path.join(folder2, filename)
        if os.path.isfile(source_file):
            unique_filename = generate_unique_filename() + os.path.splitext(filename)[1]
            destination_file = os.path.join(destination_folder, unique_filename)
            shutil.copy(source_file, destination_file)

    # Rename all files in destination folder to sample-{i}
    for i, filename in enumerate(os.listdir(destination_folder), start=1):
        old_file_path = os.path.join(destination_folder, filename)
        if os.path.isfile(old_file_path):
            new_filename = f"sample-{i}{os.path.splitext(filename)[1]}"
            new_file_path = os.path.join(destination_folder, new_filename)
            os.rename(old_file_path, new_file_path)

def main() -> None:
    # Setup argparse to accept command-line arguments
    parser = argparse.ArgumentParser(description="Copy files from two folders and rename them.")
    parser.add_argument("--first",  type=str, help="Path to the first folder", required=True)
    parser.add_argument("--second", type=str, help="Path to the second folder", required=True)
    parser.add_argument("--destination", type=str, help="Path to the destination folder", required=True)

    # Parse arguments
    arguments = parser.parse_args()

    # Call the function with arguments
    copy_and_rename_files(folder1=arguments.first, 
                          folder2=arguments.second, 
                          destination_folder=arguments.destination)

if __name__ == "__main__":
    main()
