from google.cloud import storage

def upload_to_gcs(bucket, object_name, local_file_path):
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file_path)