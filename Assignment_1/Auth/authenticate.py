'''
@author: Pruthviraj R Patil
@NetID: prp7650
'''

'''
Description: 
This class is used to protect the authentication key given in the Max code. I have copied just the key from Max code to
this class and retrieve it back using getKey method. 

Steps to use this class: 
a. In the jupyter notebook, we can just import this library as: from Auth.authenticate import Authenticate
b. create an instance: auth = Authenticate()
c. Get the key as from the class : key = auth.getKey()
d. now use the variable "key" as used earlier when exposed. But difference is the key is not exposed now.

Other changes made:
a. Install polygon using:  ! pip install polygon-api-client~=1.0.0b
b. change "from polygon import RESTClient"  to "from polygon.rest import RESTClient"

NOTE: the ipynb notebook is also submitted as a part of this assignment. Please check that out as well to run this.
'''


class Authenticate:
    def __init__(self):
        self.key = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"

    def getKey(self):
        # The api key given by the professor
        key = self.key
        return key
