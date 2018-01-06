# DatabaseToCloudsearch
A simple program to load AWS Cloudsearch domain with data from a relational database.

## Run Configuration
Use the following VM arguments to provide the necessary values to the program.
* DB_HOST: Hostname of the database (For e.g., localhost, xyzdb.com, etc.)
* DB_PORT: Port to access the database (For e.g., 3306 for MySQL)
* DB_USER: Username to access the database
* DB_PASSWORD: Password to access the database
* DB_NAME: Name of the database (For e.g., test, employeedb, etc. This is NOT the type of the database like MySQL, SQL Server, etc.)
* AWS_ACCESS_KEY_ID: Access Key ID from the AWS credentials to access AWS resources
* AWS_SECRET_ACCESS_KEY: Secret Key from the AWS credentials to access AWS resources
* AWS_CS_DOC_ENDPOINT: Document endpoint for the AWS Cloudsearch domain where data is to be uploaded
* AWS_SIGNING_REGION: AWS region (For e.g., us-east-1, eu-west-1, etc.) for the corresponding to the AWS Cloudsearch domain
* LOG_DIR: Directory to save the log files (For e.g., /tmp/app/logs/, C:\myapps\logs\, etc.)

## Notes
This application simply extracts from a database and uploads to the given AWS Cloudsearch domain. It does not provide methods to do deletes or parallel uploads, although it should be easy to change the code to do that.

## Dependencies
This application uses the Amazon AWS SDK, AWS Cloudsearch SDK, MySQL Connector and the JSON in Java (org.json) libraries.


