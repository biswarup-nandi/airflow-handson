import csv
import boto3

def read_csv(file_path):
    """
    Reads data from a CSV file and returns a list of dictionaries.
    Each dictionary represents a row in the CSV file.
    """
    data = []
    with open(file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Convert 'age' to integer, 'status' to string, and 'fullname' to string
            row['age'] = {'S': str(row['age'])}
            row['status'] = {'S': str(row['status'])}
            row['fullname'] = {'S': str(row['fullname'])}
            data.append(row)
    return data

def batch_insert_into_dynamodb(data, table_name, dynamodb_client, batch_size=25):
    """
    Inserts data into a DynamoDB table in batch mode.
    """
    # Split data into batches
    batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]
    
    # Insert each batch into DynamoDB
    for batch in batches:
        dynamodb_client.batch_write_item(
            RequestItems={
                table_name: [
                    {'PutRequest': {'Item': item}}
                    for item in batch
                ]
            }
        )

def main():
    # Specify the path to the CSV file
    csv_file = 'additional_files\data.csv'

    # Specify the name of the DynamoDB table
    dynamodb_table = 'dummyDynamoData'

    # Initialize DynamoDB client
    dynamodb_client = boto3.client('dynamodb')

    # Read data from CSV
    data = read_csv(csv_file)

    # Insert data into DynamoDB table in batch mode
    batch_insert_into_dynamodb(data, dynamodb_table, dynamodb_client)

if __name__ == "__main__":
    main()
