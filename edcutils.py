"""
Created on Feb 22, 2021

utility functions for processing catalog objects

@author: vinay thummala
"""

import requests
import json
from requests.auth import HTTPBasicAuth
import os



def getAllResource(url, user, pWd):
    """
    get the resource definition - given a resource name (and catalog url)
    catalog url should stop at port (e.g. not have ldmadmin, ldmcatalog etc...
    or have v2 anywhere
    since we are using v1 api's

    returns rc=200 (valid) & other rc's from the get
            resourceDef (json)

    """

    print("getting resource for catalog:-" + url + " user=" + user)
    apiURL = url + "/access/1/catalog/resources/"
    # print("\turl=" + apiURL)
    header = {"Accept": "application/json"}
    tResp = requests.get(
        apiURL, params={}, headers=header, auth=HTTPBasicAuth(user, pWd), verify=False
    )
    print("\tresponse=" + str(tResp.status_code))
    if tResp.status_code == 200:
        # valid - return the jsom
        return tResp.status_code, json.loads(tResp.text)
    else:
        # not valid
        return tResp.status_code, None



def getResourceDef(url, user, pWd, resourceName, sensitiveOptions=False):
    """
    get the resource definition - given a resource name (and catalog url)
    catalog url should stop at port (e.g. not have ldmadmin, ldmcatalog etc...
    or have v2 anywhere
    since we are using v1 api's

    returns rc=200 (valid) & other rc's from the get
            resourceDef (json)

    """

    print(
        "getting resource for catalog:-"
        + url
        + " resource="
        + resourceName
        + " user="
        + user
    )
    apiURL = url + "/access/1/catalog/resources/" + resourceName
    if sensitiveOptions:
        apiURL += "?sensitiveOptions=true"
    # print("\turl=" + apiURL)
    header = {"Accept": "application/json"}
    tResp = requests.get(
        apiURL, params={}, headers=header, auth=HTTPBasicAuth(user, pWd), verify=False
    )
    print("\tresponse=" + str(tResp.status_code))
    if tResp.status_code == 200:
        # valid - return the jsom
        return tResp.status_code, json.loads(tResp.text)
    else:
        # not valid
        return tResp.status_code, None


def updateResourceDef(url, user, pWd, resourceName, resJson):
    """
    update a setting in an existing resource

    returns rc=200 (valid) & other rc's from the put
            resourceDef (json)

    """

    print(
        "\tupdating resource for catalog:-"
        + url
        + " resource="
        + resourceName
        + " user="
        + user
    )
    print("\t" + json.dumps(resJson))
    apiURL = url + "/access/1/catalog/resources/" + resourceName
    print("\turl=" + apiURL)
    header = {"Accept": "application/json", "Content-Type": "application/json"}
    tResp = requests.put(
        apiURL,
        data=json.dumps(resJson),
        headers=header,
        auth=HTTPBasicAuth(user, pWd),
        verify=False,
    )
    print("\tresponse=" + str(tResp.status_code))
    if tResp.status_code == 200:
        # valid - return the jsom
        print("\tyay - update resource worked...")
        print(tResp)
        return tResp.status_code
    else:
        # not valid
        print("\tdarn - update resource failed...")
        print(tResp)
        return tResp.status_code



def createResource(url, user, pWd, resourceName, resourceJson):
    """
    create a new resource based on the provided JSON

    returns rc=200 (valid) & other rc's from the put
            resourceDef (json)

    """
    # create a new resource
    apiURL = url + "/access/1/catalog/resources/"
    header = {"content-type": "application/json"}
    print("\tcreating resource: " + resourceName)
    newResourceResp = requests.post(
        apiURL,
        data=json.dumps(resourceJson),
        headers=header,
        auth=HTTPBasicAuth(user, pWd),
        verify=False,
    )
    print("\trc=" + str(newResourceResp.status_code))
    print("\tbody=" + str(newResourceResp.text))

    return newResourceResp.status_code


def uploadResourceFile(url, user, pWd, resourceName, fileName, fullPath, scannerId):
    """
    upload a file for the resource - e.g. a custom lineage csv file
    works with either csv for zip files  (.csv|.zip)

    returns rc=200 (valid) & other rc's from the post

    """
    print(
        "uploading file for resource "
        + url
        + " resource="
        + resourceName
        + " user="
        + user
    )
    apiURL = url + "/access/1/catalog/resources/" + resourceName + "/files"
    print("\turl=" + apiURL)
    # header = {"accept": "*/*", "Content-Type" : "multipart/form-data"}
    header = {"accept": "*/*"}
    print("\t" + str(header))
    # params={"scannerid": "LineageScanner", "filename": fileName, "optionid": "File"}
    params = {"scannerid": scannerId, "filename": fileName, "optionid": "File"}
    print("\t" + str(params))
    #     files = {'file': fullPath}
    mimeType = "text/csv"
    readMode = "rt"
    if fileName.endswith(".zip"):
        mimeType = "application/zip"
        readMode = "rb"

    if fileName.endswith(".dsx"):
        mimeType = "text/plain"

    file = {"file": (fileName, open(fullPath, readMode), mimeType)}
    print("\t" + str(file))
    uploadResp = requests.post(
        apiURL,
        data=params,
        files=file,
        headers=header,
        auth=HTTPBasicAuth(user, pWd),
        verify=False,
    )
    print("\tresponse=" + str(uploadResp.status_code))
    if uploadResp.status_code == 200:
        # valid - return the jsom
        return uploadResp.status_code
    else:
        # not valid
        print("\tupload file failed")
        print("\t" + str(uploadResp))
        print("\t" + str(uploadResp.text))
        return uploadResp.status_code


def executeResourceLoad(url, user, pWd, resourceName):
    """
    start a resource load

    returns rc=200 (valid) & other rc's from the get
            json with the job details

    """

    print(
        "starting scan resource " + url + " resource=" + resourceName + " user=" + user
    )
    apiURL = url + "/access/2/catalog/resources/jobs/loads"
    print("\turl=" + apiURL)
    header = {"accept": "application/json", "Content-Type": "application/json"}
    print("\t" + str(header))
    params = {"resourceName": resourceName}
    print("\t" + str(params))
    uploadResp = requests.post(
        apiURL,
        data=json.dumps(params),
        headers=header,
        auth=HTTPBasicAuth(user, pWd),
        verify=False,
    )
    print("\tresponse=" + str(uploadResp.status_code))
    if uploadResp.status_code == 200:
        # valid - return the jsom
        return uploadResp.status_code, json.loads(uploadResp.text)
    else:
        # not valid
        print("\tdarn - resource start failed")
        print("\t" + str(uploadResp))
        print("\t" + str(uploadResp.text))
        return uploadResp.status_code, None

## To Do Job Status

def callGETRestEndpoint(apiURL, user, pWd):
    """
    this function call the URL  with a GET method and return the status code
    as well as the response body
    returns rc=200 (valid) & other rc's from the get
            resourceDef (json)
    """
    header = {"Accept": "application/json"}
    tResp = requests.get(
        apiURL, params={}, headers=header, auth=HTTPBasicAuth(user, pWd), verify=False
    )
    print("\tresponse=" + str(tResp.status_code))
    if tResp.status_code == 200:
        # valid - return the jsom
        return tResp.status_code, json.loads(tResp.text)
    else:
        # not valid
        return tResp.status_code, None


def getResourceObjectCount(url, user, pWd, resourceName):
    """
    get the resource object count - given a resource name (and catalog url)
    """

    apiURL = url + "/access/2/catalog/data/objects?q=core.resourceName:" + resourceName
    print(
        "getting object count for catalog resource:-"
        + apiURL
        + " resource="
        + resourceName
        + " user="
        + user
    )
    return callGETRestEndpoint(apiURL, user, pWd)


def getCatalogObjectCount(url, user, pWd):
    """
    get the resource object count - given a catalog url
    """

    print("getting object count for catalog resource:-" + url + " user=" + user)
    apiURL = url + "/access/2/catalog/data/objects"
    return callGETRestEndpoint(apiURL, user, pWd)


def getCatalogResourceCount(url, user, pWd):
    """
    get the resource count - given a catalog url
    """

    apiURL = url + "/access/2/catalog/data/objects?q=core.allclassTypes:core.Resource"
    print("getting object count for catalog resource:-" + apiURL + " user=" + user)
    return callGETRestEndpoint(apiURL, user, pWd)



