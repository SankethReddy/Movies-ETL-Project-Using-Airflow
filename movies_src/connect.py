# -*- coding: utf-8 -*-
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
import binascii

class Connect:

    def processConnection(self):
        with open('/opt/airflow/data/encryptionMySQL.txt', 'r') as eFile:
            e = eFile.read()
        
        with open('/opt/airflow/data/private_keyMySQL.pem', 'rb') as kFile:
            pk = serialization.load_pem_private_key(kFile.read(), 
                                                    password=None,
                                                    backend=default_backend()
                                                    )
        
        connection_string = pk.decrypt(
        binascii.unhexlify(str.encode(e)),
        padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()),
                        algorithm=hashes.SHA256(),
                        label=None
                        )
            )
        
        connection_string = connection_string.decode('utf-8')
        return connection_string