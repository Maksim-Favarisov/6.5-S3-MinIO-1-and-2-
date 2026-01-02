import boto3
import json
from botocore.client import Config
from botocore.exceptions import ClientError


class S3Client:
    def __init__(self, endpoint, access_key, secret_key, bucket):
        """
        Инициализация клиента для работы с S3-совместимым хранилищем.
        """
        self.bucket = bucket

        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint,            # URL S3-хранилища (Selectel / MinIO / Yandex)
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name="us-east-1",
            # verify=False                    # для Selectel
        )

    def set_bucket_policy(self):
        """Настройка bucket policy"""

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                # Анонимное чтение (работает, скачивает по ссылке: http://localhost:9000/bucketmaks/sales_data.csv)
                {
                    "Sid": "PublicRead",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{self.bucket}/*"
                },
                # Полный доступ для user_m (для примера, по факту у MinIO плохая совместимость, не работает)
                {
                    "Sid": "FullAccessForUserM",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "user_m"
                    },
                    "Action": "s3:*",
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket}",
                        f"arn:aws:s3:::{self.bucket}/*"
                    ]
                },
                # Только чтение для user_b (для примера, по факту у MinIO плохая совместимость, не работает)
                {
                    "Sid": "ReadOnlyForUserB",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "user_b"
                    },
                    "Action": [
                        "s3:GetObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket}",
                        f"arn:aws:s3:::{self.bucket}/*"
                    ]
                }
            ]
        }

        self.s3.put_bucket_policy(
            Bucket=self.bucket,
            Policy=json.dumps(policy)
        )
        print("✅ Политика обновлена с конкретными пользователями")


    def enable_bucket_versioning(self):
        """Включение версионирования для бакета"""
        try:
            self.s3.put_bucket_versioning(
                Bucket=self.bucket,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            print(f"✅ Версионирование включено для бакета '{self.bucket}'")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                print(f"❌ Бакет '{self.bucket}' не найден")
            elif error_code == 'AccessDenied':
                print(f"❌ Доступ запрещен для бакета '{self.bucket}'")
            else:
                print(f"❌ Ошибка включения версионирования: {error_code}")
            return False


    def set_bucket_lifecycle_policy(self, expiration_days=3):
        """
        Настройка политики жизненного цикла для бакета
        """
        lifecycle_config = {
            'Rules': [{
                'Status': 'Enabled',
                'Filter': {'Prefix': ''},  # Применяется ко всем объектам
                'Expiration': {'Days': expiration_days}
            }]
        }

        try:
            self.s3.put_bucket_lifecycle_configuration(
                Bucket=self.bucket,
                LifecycleConfiguration=lifecycle_config
            )
            print(f"✅ Lifecycle policy настроена успешно")
            print(f"   Объекты в бакете '{self.bucket}' будут автоматически удаляться через {expiration_days} дней")
            return True

        except Exception as e:
            print(f"❌ Ошибка настройки lifecycle policy: {e}")
            return False
    # ==========================
    # Базовые методы (пример)
    # ==========================

    def upload(self, file_path, object_name):
        """
        Загружает файл в бакет.
        """
        self.s3.upload_file(file_path, self.bucket, object_name)
        print(f"Загружено: {object_name}")

    def download(self, object_name, save_path):
        """
        Скачивает объект из S3.
        """
        self.s3.download_file(self.bucket, object_name, save_path)
        print(f"Скачано: {object_name}")

    # ==========================
    # Методы из задания
    # ==========================

    def list_files(self):
        """
        Возвращает список всех объектов в бакете.
        """
        response = self.s3.list_objects_v2(Bucket=self.bucket)
        if "Contents" not in response:
            return []

        return [obj["Key"] for obj in response["Contents"]]

    def file_exists(self, object_name):
        """
        Проверяет существование объекта в бакете. Возвращает True/False.
        """
        try:
            self.s3.head_object(Bucket=self.bucket, Key=object_name)
            return True
        except ClientError:
            return False