"""
    File name: InformaticaAPI.py
    Author: Cameron Wonchoba
    Date created: 10/6/2020/
    Date last modified: 11/3/2020
    Python Version: 3.6+
"""

import copy
import pandas as  pd
import os
import re

from Connection import Connection

class InformaticaAPI(Connection):
    """
    A class used to run common API requests on EDC. Extends the Connection class

    ...
    
    Attributes
    ----------
    None

    Methods
    -------
    getObject(objectID)
        Get details about a specific object. Return a JSON object
    search(query, offset)
        Search for objects based on query.
    updateObject: update specific object
    """
    
    def __init__(self, securityDomain=None, userName=None, password=None, catalogService=None, verbose=False):
        """Constructor - Set-up connection using Parent Class"""
        # Call super class
        super().__init__(securityDomain, userName, password, catalogService, verbose)

    
    def getObject(self, objectID):
        """
        Get details about a specific object. Return a JSON object
        
        Parameters
        ----------
        objectID : String 
            The objectID. This can be found in the URL when viewing a particular object
            e.g.: <catalogService>/ldmcatalog/main/ldmObjectView/('$obj':'<objectID>','$type':com.infa.ldm.relational.Table,'$where':ldm.ThreeSixtyView)
            The objectID <objectID>
        """
        # Convert objectID to safe encoding
        objectID = self.encodeID(objectID, tilde=True)

        url = f"{self.catalogService}/access/2/catalog/data/objects/{objectID}?includeRefObjects=true"
        return Connection.getResponseJSON(self, url)

    def search(self, query, offset=0):
        """Search for objects based on query
        
        Parameters
        ----------
        query: String
            The query to pass the search functionality
        offset: int, optional (default=0)
            How many items to offset the results by
        """
        url = f"{self.catalogService}/access/2/catalog/data/search?q={query}&facet=false&defaultFacets=true&highlight=false&offset={offset}&pageSize=5&enableLegacySearch=false&disableSemanticSearch=false&includeRefObjects=false"
        return Connection.getResponseJSON(self, url)
 

    def updateObject(self, objectID, data):
        """
        Updates an object with the data

        Parameters
        ----------

        objectID : String
            ObjectID to modify
        data : dictionary
            Data to put into the object
        """
        if isinstance(data, dict):
            objectID = self.encodeID(objectID, tilde=True)

            url = f"{self.catalogService}/access/2/catalog/data/objects/{objectID}"
            Connection.putRequest(self, url, data)
        else:
            print("[ERROR] - Unable to put data. Data must be in a dictionary format")
    
if __name__ == "__main__":
    """Testing APIRequest - View Examples folder for more examples"""
    InformaticaAPIObj = InformaticaAPI(verbose=True)
