# src/core/s3_deleter.py
from __future__ import annotations

from typing import List, Tuple, Dict
from botocore.exceptions import ClientError


class S3Deleter:
    """
    Delete current objects or specific versions / delete markers.
    """

    def __init__(self, boto3_client):
        self.s3 = boto3_client

    def delete_current(self, bucket: str, keys: List[str], batch_size: int = 1000) -> Tuple[int, List[Dict]]:
        """
        Keys only: this will place delete markers if versioning is enabled.
        """
        deleted_total = 0
        errors = []
        for i in range(0, len(keys), batch_size):
            batch = keys[i:i+batch_size]
            try:
                resp = self.s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True}
                )
                deleted_total += len(resp.get("Deleted", []))
                for e in resp.get("Errors", []):
                    errors.append({"Key": e.get("Key"), "Code": e.get("Code"), "Message": e.get("Message")})
            except ClientError as e:
                msg = str(e)
                for k in batch:
                    errors.append({"Key": k, "Code": "ClientError", "Message": msg})
            except Exception as e:
                msg = str(e)
                for k in batch:
                    errors.append({"Key": k, "Code": "Exception", "Message": msg})
        return deleted_total, errors

    def delete_versions(self, bucket: str, items: List[Dict], batch_size: int = 1000) -> Tuple[int, List[Dict]]:
        """
        items: list of {"Key": ..., "VersionId": ...}
        Used for permanently deleting a specific object version or a delete marker.
        """
        deleted_total = 0
        errors = []
        for i in range(0, len(items), batch_size):
            batch = items[i:i+batch_size]
            try:
                resp = self.s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": it["Key"], "VersionId": it["VersionId"]} for it in batch],
                            "Quiet": True}
                )
                deleted_total += len(resp.get("Deleted", []))
                for e in resp.get("Errors", []):
                    errors.append({
                        "Key": e.get("Key"),
                        "VersionId": e.get("VersionId"),
                        "Code": e.get("Code"),
                        "Message": e.get("Message")
                    })
            except ClientError as e:
                msg = str(e)
                for it in batch:
                    errors.append({"Key": it["Key"], "VersionId": it["VersionId"], "Code": "ClientError", "Message": msg})
            except Exception as e:
                msg = str(e)
                for it in batch:
                    errors.append({"Key": it["Key"], "VersionId": it["VersionId"], "Code": "Exception", "Message": msg})
        return deleted_total, errors