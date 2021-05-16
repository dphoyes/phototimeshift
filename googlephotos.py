#!/usr/bin/env python3

import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import AuthorizedSession

PROJECT_ROOT = os.path.dirname(os.path.realpath(__file__))


def make_session():
    credentials = None
    try:
        with open(os.path.join(PROJECT_ROOT, "gphotos-user-creds.pickle"), 'rb') as f:
            credentials = pickle.load(f)
    except FileNotFoundError:
        pass
    if credentials is None:
        flow = InstalledAppFlow.from_client_secrets_file(
            os.path.join(PROJECT_ROOT, "gphotos-app-creds.json"),
            scopes=[
                "https://www.googleapis.com/auth/userinfo.email",
                "https://www.googleapis.com/auth/userinfo.profile",
                "https://www.googleapis.com/auth/photoslibrary.readonly",
            ],
        )
        credentials = flow.run_local_server()
        with open(os.path.join(PROJECT_ROOT, "gphotos-user-creds.pickle"), 'wb') as f:
            pickle.dump(credentials, f)

    return AuthorizedSession(credentials)


def iter_august_photos(session):
    page_token = None
    while True:
        body = {
            "pageSize": 100,
            "filters": {
                "dateFilter": {
                    "dates": [
                        {
                        "day": 0,
                        "month": 8,
                        "year": 2018
                        }
                    ]
                }
            }
        }
        if page_token is not None:
            body["pageToken"] = page_token
        result = session.post("https://photoslibrary.googleapis.com/v1/mediaItems:search", json=body).json()
        yield from result['mediaItems']
        try:
            page_token = result['nextPageToken']
        except KeyError:
            return


def iter_all_photos(session):
    page_token = None
    while True:
        body = {
            "pageSize": 100,
        }
        if page_token is not None:
            body["pageToken"] = page_token
        result = session.get("https://photoslibrary.googleapis.com/v1/mediaItems", params=body).json()
        yield from result['mediaItems']
        try:
            page_token = result['nextPageToken']
        except KeyError:
            return


def main():
    session = make_session()

    for photo in iter_all_photos(session):
        filename = photo['filename']
        metadata = photo['mediaMetadata']
        photovideo = metadata.get('photo')
        if photovideo is None:
            photovideo = metadata['video']
        try:
            camera = f"{photovideo['cameraMake']} {photovideo['cameraModel']}"
        except KeyError:
            camera = "<Unknown camera>"
        if metadata['creationTime'].startswith('2017'):
            break

        # print(f"{filename}: {camera}, {metadata['creationTime']}")
        print(f"{filename}")


if __name__ == "__main__":
    main()
