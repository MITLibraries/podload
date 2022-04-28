import os

import boto3
import pytest
from botocore.exceptions import ClientError
from moto.core import set_initial_no_auth_action_count

from podload import s3


def test_s3_check_permissions_success(mocked_s3, s3_class):
    s3_class.put_file(
        "<marcXML></marcXML>",
        "podload",
        "test.xml",
    )
    result = s3_class.check_permissions("podload")
    assert (
        result
        == "S3 list objects and get object permissions confirmed for bucket: podload"
    )


@set_initial_no_auth_action_count(1)
def test_s3_check_permissions_raises_error_if_no_permission(
    mocked_s3, s3_class, aws_test_user
):
    s3_class.put_file(
        "<marcXML></marcXML>",
        "podload",
        "test.xml",
    )
    os.environ["AWS_ACCESS_KEY_ID"] = aws_test_user["AccessKeyId"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_test_user["SecretAccessKey"]
    boto3.setup_default_session()
    s3_class = s3.S3()
    with pytest.raises(ClientError) as e:
        s3_class.check_permissions("podload")
    assert (
        "An error occurred (AccessDenied) when calling the GetObject operation: Access "
        "Denied" in str(e.value)
    )


def test_archive_file_in_bucket(mocked_s3, s3_class):
    s3_class.put_file(
        "<marcXML></marcXML>",
        "podload",
        "test.xml",
    )
    s3_class.archive_file_with_new_key(
        "podload",
        "test.xml",
        "archived",
    )
    with pytest.raises(ClientError) as e:
        response = s3_class.client.get_object(Bucket="podload", Key="test.xml")
    assert (
        "An error occurred (NoSuchKey) when calling the GetObject operation: The"
        " specified key does not exist." in str(e.value)
    )
    response = s3_class.client.get_object(Bucket="podload", Key="archived/test.xml")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_s3_filter_files_in_bucket_with_matching_file(mocked_s3, s3_class):
    s3_class.put_file(
        "<marcXML></marcXML>",
        "podload",
        "upload/pod.xml",
    )
    files = s3_class.filter_files_in_bucket("podload", "xml", "upload/")
    for file in files:
        assert file == "upload/pod.xml"


def test_s3_filter_files_in_bucket_without_matching_file(mocked_s3, s3_class):
    with pytest.raises(StopIteration):
        s3_class.put_file(
            "<marcXML></marcXML>",
            "podload",
            "pod.xml",
        )
        files = s3_class.filter_files_in_bucket("podload", "xml", "upload/")
        next(files) == "test.csv"


def test_s3_put_file(mocked_s3, s3_class):
    assert "Contents" not in s3_class.client.list_objects(Bucket="podload")
    s3_class.put_file(
        "<marcXML></marcXML>",
        "podload",
        "test.xml",
    )
    assert len(s3_class.client.list_objects(Bucket="podload")["Contents"]) == 1
    assert (
        s3_class.client.list_objects(Bucket="podload")["Contents"][0]["Key"]
        == "test.xml"
    )
