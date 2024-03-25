from botocore.exceptions import ClientError
import boto3
import tempfile
import zipfile
import re
import click


class ExportDir(object):
    def __init__(self, bucket_name, folder_name):
        self._bucket_name = bucket_name
        self._folder_name = folder_name

    @property
    def bucket_name(self):
        return self._bucket_name

    @property
    def folder_name(self):
        return self._folder_name


class S3ExportDirParam(click.types.ParamType):
    name = "s3_uri"

    def convert(self, value, param, ctx):
        match = re.match(r"^s3://(?P<bucket>[^/]*)/(?P<folder>.*)$", value)

        if match is None:
            self.fail("Invalid s3 URI", param, ctx)

        bucket_name = match.group("bucket")
        folder_name = match.group("folder")
        if folder_name[-1] != "/":
            folder_name += "/"

        return ExportDir(bucket_name, folder_name)


@click.command()
@click.argument("s3_uri", type=S3ExportDirParam())
@click.argument("output", type=click.types.Path())
def cli(**kwargs):
    """
    Create zip file containing export content data
    Note:
    """

    bucket_name = kwargs['s3_uri'].bucket_name
    folder_name = kwargs['s3_uri'].folder_name
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)

    # Get manifest
    try:
        click.echo("Reading manifest...")
        manifest_obj = bucket.Object(f"{folder_name}manifest.txt")
        manifest = manifest_obj.get()['Body'].readlines()
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            raise click.exceptions.BadParameter("given s3 directory does not contain manifest.txt")
        else:
            raise e

    # Crate zip file
    with zipfile.ZipFile(kwargs['output'], 'w', compression=zipfile.ZIP_DEFLATED) as package:

        # zip manifest
        with tempfile.NamedTemporaryFile("wb") as tmp:
            click.echo(f"adding manifest to zip")
            manifest_obj.download_fileobj(tmp)
            package.write(tmp.name, manifest_obj.key)

        # Create iterator
        total_count = len(manifest)
        finished_count = 0
        for line in manifest:
            try:
                _, path = line.decode("utf-8").split(":")
                path = path.strip()
                obj = bucket.Object(path)
                with tempfile.NamedTemporaryFile("wb") as tmp:
                    click.echo(f"adding {obj.key}")
                    obj.download_fileobj(tmp)
                    package.write(tmp.name, obj.key)
            except Exception as e:
                raise e
            finally:
                finished_count += 1
                click.echo(f"{finished_count / total_count * 100:0.0f}% ({finished_count}/{total_count})...")


if __name__ == "__main__":
    cli()
