from podload.cli import add_namespaces_to_alma_xml, extract_files_from_tar, main


def test_main_success(mocked_pod, mocked_s3, runner, s3_class):
    s3_class.put_file(
        open("tests/fixtures/pod.tar.gz", "rb"),
        "podload",
        "upload/pod.tar.gz",
    )
    result = runner.invoke(
        main,
        [
            "--bucket",
            "podload",
            "--file_extension",
            ".tar.gz",
            "--key_prefix",
            "upload",
            "--file_mimetype",
            "application/xml",
            "--org",
            "uni",
            "--stream",
            "test_stream",
            "--url",
            "http://example.example",
        ],
    )
    print(result.output)
    print(result.exception)
    assert result.exit_code == 0


def test_add_namespaces_to_alma_xml():
    xml_element = add_namespaces_to_alma_xml(open("tests/fixtures/pod.xml", "rb"))
    assert xml_element.attrib == {
        "xmlns": "http://www.loc.gov/MARC21/slim",
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xsi:schemaLocation": (
            "http://www.loc.gov/MARC21/slim "
            + "http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"
        ),
    }


def test_extract_files_from_tar():
    files = extract_files_from_tar(open("tests/fixtures/pod.tar.gz", "rb"))
    for file in files:
        assert file.read() == open("tests/fixtures/pod.xml", "rb").read()
