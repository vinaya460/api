"""
    File name: EDCTools.py
    Author: Cameron Wonchoba
    Date created: 10/6/2020
    Date last modified: 11/4/2020
    Python Version: 3.6+
"""


from InformaticaAPI import InformaticaAPI

import pandas as pd
import os

class EDCTools(InformaticaAPI):
    """
    A class used to extract a variety of information from EDC
    ...
    
    Attributes
    ----------
    directory - String
        Holds the name of a directory we may write to
            
    Methods
    -------
    searchObject(objectName, objectType, maxHits=250)
        Search for the ID of the objectName
    extractTables(self, df_attributes)
        Extract all table dependencies
    extractColumns(df_attributes)
        Extract column dependenncies
    extractTableColumns(objectID)
        Extract columns from a table
    """

    def __init__(self, securityDomain=None, userName=None, password=None, catalogService=None, verbose=False):
        super().__init__(securityDomain, userName, password, catalogService, verbose)
        self.directory = None

    def searchObject(self, objectName, objectType, maxHits=250):
        """Search for the ID of an object
        
        Parameters
        ----------
        objectName: String
            The name of the object to search for
        objectType : String
            The type of the object to search for
        maxHits : int (Default = 250)
            The maximum number of hits to search through
        """
        offsetCounter = 0
        hits = self.search(objectName, offset=offsetCounter)['hits']
        # Check name is same and we are dealing with the correct type.
        while len(hits) > 0 and offsetCounter <= maxHits: # If we can't find it after 250 hits, stop looking.
            for hit in hits:
                offsetCounter+=1
                nameCheck=False
                classTypeCheck=False
                objectID = hit['id']

                values = hit['values']
                for value in values:
                    if value['attributeId'] == 'core.name' and value['value'].lower() == objectName.lower():
                        nameCheck = True
                    elif value['attributeId'] == 'core.classType' and value['value'] == objectType:
                        classTypeCheck = True

                if nameCheck and classTypeCheck and self.verbose:
                    print("Match found!")
                    print(objectName, " : ", objectID)
                    return objectID # Return objectID

            hits = self.search(objectName, offset=offsetCounter)['hits']

    
    def extractTables(self, df_attributes):
        """Extract all table dependencies
        
        Parameters
        ----------
        df_attributes: DataFrame
            DataFrame the contains details about each Data Object within a Lineage or Impact
        """
        # Only actual tables. Not views or temporary tables 
        df = df_attributes[df_attributes['level_1_core.classType'] == "com.infa.ldm.relational.Table"][['level_1_core.name', 'ID']]
        df.columns = ["Table", "Path"]
        for idx, row in df.iterrows():
            df.at[idx, "Path"] = row["Path"].rsplit("/",1)[0]
        
        df = df.drop_duplicates(subset='Path', keep='last')
        return df

    def extractColumns(self, df_attributes):
        """Extract column dependenncies
        
        Parameters
        ----------
        df_attributes: DataFrame
            DataFrame the contains details about each Data Object within a Lineage or Impact
        """
        # Only extracting columns from tables. Not views or temp
        df = df_attributes[df_attributes['level_2_core.classType'] == "com.infa.ldm.relational.Column"][['level_1_core.name', 'level_2_core.name', 'ID']]
        df.columns = ['Table','Column','Path']
        return df

    

    def extractTableColumns(self, objectID):
        """
        Extract columns from a table
        
        Parameters
        ----------
        objectID : String
            Object ID of object we want to extract columns for.
        """
        # TODO: We might want to pass in a database..
        response = self.getObject(objectID)

        verifyTable = False
        # Ensure that the object was a table
        if 'facts' in response:
            for fact in response['facts']:
                if fact['attributeId'] == 'core.classType':
                    if fact['value'] == 'com.infa.ldm.relational.Table' or fact['value'] == 'com.infa.ldm.relational.View':
                        verifyTable = True
                    break

            if verifyTable:
                columns = []
                # Extract columns
                for fact in response['dstLinks']:
                    if fact['classType'] == 'com.infa.ldm.relational.Column' or fact['classType'] == 'com.infa.ldm.relational.ViewColumn':
                        columns += [fact['name']]

                return sorted(columns)
        print("[ERROR] - Passed in ID of Object that isn't a table: ", objectID)
        return []

if __name__ == "__main__":
    EDCToolsObj = EDCTools(verbose=True)
