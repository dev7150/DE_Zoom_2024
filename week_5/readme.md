Ran into this strange issue where jupyter notebook webpage hosted in google ubuntu VM was not loading in local browser even after forwarding the port. Needed to create a tunnel using the following command to get arounf the isse.

` ssh -i C:/Users/Dev.Poudel/.ssh/dezoom -L 8888:localhost:8888 suzu.sharma@34.129.43.225`
