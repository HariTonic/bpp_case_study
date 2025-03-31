from io import StringIO

import boto3


class S3:
    def __init__(
        self, aws_access_key=None, aws_secret_key=None, region_name="us-east-1"
    ):
        """
        Initialize the S3Uploader class with optional AWS credentials.

        Parameters:
        - aws_access_key: AWS Access Key ID (optional)
        - aws_secret_key: AWS Secret Access Key (optional)
        - region_name: AWS Region Name (default is 'us-east-1')
        """
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.region_name = region_name

    def save_df_to_s3(self, df, bucket_name, s3_file_name):
        """
        Saves a DataFrame as a CSV file to an S3 bucket.

        Parameters:
        - df: DataFrame to be saved
        - bucket_name: Name of the S3 bucket
        - s3_file_name: File name to be used for the CSV in S3
        """
        # Create an S3 client
        if self.aws_access_key and self.aws_secret_key:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name=self.region_name,
            )
        else:
            s3 = boto3.client("s3", region_name=self.region_name)

        # Convert DataFrame to CSV in memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload CSV to S3
        s3.put_object(Bucket=bucket_name, Body=csv_buffer.getvalue(), Key=s3_file_name)

        print(f"File saved to S3: s3://{bucket_name}/{s3_file_name}")
