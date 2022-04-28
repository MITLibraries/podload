import logging
import os
import tarfile

import click
import requests
import smart_open
from defusedxml import ElementTree as ET

from podload.s3 import S3

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--bucket",
    required=True,
)
@click.option(
    "--file_extension",
    required=True,
)
@click.option(
    "--file_mimetype",
    required=True,
)
@click.option(
    "--key_prefix",
    required=True,
)
@click.option(
    "--org",
    required=True,
)
@click.option(
    "--stream",
    required=True,
)
@click.option(
    "--url",
    required=True,
)
def main(bucket, file_extension, key_prefix, file_mimetype, org, stream, url):
    headers = {"Authorization": f'Bearer {os.getenv("ACCESS_TOKEN")}'}
    url = f"{url}/{org}/uploads?stream={stream}"

    s3 = S3()
    s3_files = s3.filter_files_in_bucket(bucket, file_extension, key_prefix)
    for s3_file in s3_files:
        print(s3_file)
        s3_file_content = smart_open.open(f"s3://{bucket}/{s3_file}", "rb")
        files = extract_files_from_tar(s3_file_content)
        for file in files:
            xml_element = add_namespaces_to_alma_xml(file)
            files = {
                "upload[files][]": (
                    os.path.basename(s3_file).replace("tar.gz", "xml"),
                    ET.tostring(xml_element, encoding="utf8", method="xml"),
                    file_mimetype,
                ),
            }
            response = requests.post(url, files=files, headers=headers)
            response.raise_for_status()
            print(response)
            # To do: archive files to avoid dupe uploads


def add_namespaces_to_alma_xml(file):
    xml_element = ET.fromstring(file.read().decode("utf-8"))
    xml_element.attrib["xmlns"] = "http://www.loc.gov/MARC21/slim"
    xml_element.attrib["xmlns:xsi"] = "http://www.w3.org/2001/XMLSchema-instance"
    xml_element.attrib["xsi:schemaLocation"] = (
        "http://www.loc.gov/MARC21/slim "
        + "http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"
    )
    return xml_element


def extract_files_from_tar(tar_file):
    tar = tarfile.open(fileobj=tar_file)
    for member in tar.getmembers():
        file = tar.extractfile(member)
        if file is not None:
            yield file
