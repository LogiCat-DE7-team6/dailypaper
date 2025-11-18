import boto3
from botocore.exceptions import ClientError

class S3Service:
    def __init__(self, **kwargs):
        # kwargs에서 값을 꺼내서 사용
        self.bucket_name = kwargs.get('bucket_name')
        self.s3_access_key = kwargs.get('s3_access_key')
        self.s3_secret_key = kwargs.get('s3_secret_key')
        region_name = kwargs.get('region_name', 'us-west-2')  # 기본값 'us-west-2'

        # boto3 client 초기화
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
            region_name=region_name
        )

    def upload_file(self, **kwargs):
        try:
            file_path = kwargs.get("file_path")
            object_key = kwargs.get("object_key")
            self.s3.upload_file(file_path, self.bucket_name, object_key)
            print("S3 파일 업로드 성공!")
            return True
        except ClientError as e:
            print(f"S3 Upload Error: {e}")
            raise e

    def generate_presigned_url(self, object_key, expires_in=3600):
        return self.s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': self.bucket_name, 'Key': object_key},
            ExpiresIn = expires_in
        )