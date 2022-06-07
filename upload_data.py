# Import
import os
from azure.storage.filedatalake import DataLakeServiceClient

# Put the name of my container that I created
container_name = "blob-container-01"

# Connect with my connection string
# Set my environnement variable
connection_string = os.environ.get("connection_string", None)
datalake_service_client = DataLakeServiceClient.from_connection_string(connection_string)

# Create my function to upload files in my directory
def upload_files(my_dir, azure_dir=None, rec=False):
    if(azure_dir):
        azure_dir = azure_dir + "/"
    else:
        azure_dir= " "
    
    os.chdir(my_dir)
    for file in os.listdir(os.getcwd()):
        if(rec and os.path.isdir(file)):
            upload_files(file, azure_dir + file)
        else:
            file_client = datalake_service_client.get_file_client(container_name, azure_dir + file)
            file_client.create_file()

            with open(file, 'rb') as f:
                file_content = f.read()
                file_client.append_data(data = file_content, offset=0, length=len(file_content))
                file_client.flush_data(len(file_content))
                print("file ("+ file +") uploaded to directory " + azure_dir)
    os.chdir("..")

file_system_client = None
file_systems = datalake_service_client.list_file_systems()
for file_system in file_systems:
    if file_system.name == container_name:
        file_system_client = datalake_service_client.get_file_system_client(container_name)


# Call my function with the link of my dataset
upload_files("C:/Users/emmab/OneDrive/Documents/DATALAKES/dataset")
