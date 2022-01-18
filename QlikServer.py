import requests, requests_ntlm, websocket, json, shutil, asyncio, pyodbc
requests.packages.urllib3.disable_warnings()

class QlikSenseServer:
    
    def __init__ (self, url):
        self.session = requests.Session()
        self.session_id = None;
        self.url = url
        self.wss = url.replace('http', 'ws')
        self.XREF = "abcdefghijklmnop"
        self.URL_XREF_POSTFIX = f'xrfkey={self.XREF}'
        self.NTLM_HEADERS = {
            "X-Qlik-XrfKey": self.XREF,
            "Accept": "application/json",
            "X-Qlik-User": "UserDirectory=Internal;UserID=sa_repository",
            "Content-Type": "application/json",
            "Connection": "Keep-Alive",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36"
        }
        self.cookies = None;
        self._engineSocket = None;
        
        
        
        
    def __del__ (self):
        pass
        #self.disconnect()
        
    def connect(self, login, password, auth_method='NTLM'):
        self.session.auth = requests_ntlm.HttpNtlmAuth(login, password, self)
        self.auth_get = self.session.get(f'{self.url}/qrs/about?{self.URL_XREF_POSTFIX}' , verify=False, headers=self.NTLM_HEADERS)
        self.cookies = self.session.cookies.get_dict()
        self.session_id = self.session.cookies.get('X-Qlik-Session')
        self._engineSocket = websocket.WebSocket()
        socket_connect_url = f"{self.wss}/app/engineData"
        
        self._engineSocket.connect(socket_connect_url,
            header={"X-Qlik-User":"UserDirectory=internal; UserId=sa_engine"},
            cookie=f'X-Qlik-Session={self.session_id}')
        
        
        return self.auth_get.content
        
    def disconnect(self):
        self.session.close()
        return self.session
    
    def get_apps(self): 
        endpoint = 'qrs/app/full'
        apps = self.session.get(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}', verify=False, headers=self.NTLM_HEADERS)
        return apps.json()
    
    def get_tasks(self): 
        endpoint = 'qrs/task/full'
        tasks = self.session.get(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}', verify=False, headers=self.NTLM_HEADERS)
        return tasks.json()
        
    def get_rules(self):
        endpoint = 'qrs/systemrule/full'
        rules = self.session.get(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}', verify=False, headers=self.NTLM_HEADERS)
        return rules.json()
    
    def get_users(self):
        endpoint = 'qrs/user/full'
        users = self.session.get(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}', verify=False, headers=self.NTLM_HEADERS)
        return users
    
    def create_rule(self, rule): 
        endpoint = '/qrs/systemrule'
        
        keys = ["category","name","rule","actions", "resourceFilter",
                'comment', 'disabled', 'ruleContext', 'tags']
        rule = { key: rule[key] for key in keys }
        create_rule = self.session.post(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}', verify = False, data=json.dumps(rule), headers = self.NTLM_HEADERS)
        
    def get_importfolder(self):
        endpoint = 'qrs/app/importfolder'
        req = self.session.get(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}', verify = False, headers = self.NTLM_HEADERS )
        return req.content
    
    def export_app(self, id=None, name=None, with_data=True):
        skipData = 'true' if not with_data else 'false'
        endpoint = "qrs/app"
        download_app = self.session.post(f'{self.url}/{endpoint}/{id}/export/{id}?{self.URL_XREF_POSTFIX}&skipdata={skipData}', verify=False, headers=self.NTLM_HEADERS)
        return download_app.json()
    
    def import_app(self, file_name, new_name = None):
        #first apps need to be placed in the import folder. Run get_importfolder() to get the path
        
        new_name = new_name if new_name else file_name.split('.qvf')[0]
        
        #destination = self.__get_importfolder()
        data = f'"{file_name}"'
        endpoint = 'qrs/app/import'
        import_req = self.session.post(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}&name={new_name}', verify=False, headers= self.NTLM_HEADERS, data=data )
        return import_req.json()
    
    def get_app_objects(self): 
        endpoint = 'qrs/app/object/full'
        object_req = self.session.get(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}', verify=False, headers= self.NTLM_HEADERS )
        return object_req

    def publish_object(self, id):
        endpoint = f'qrs/app/object/{id}/publish'
        publish_req = self.session.put(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}', verify=False, headers= self.NTLM_HEADERS )
        return publish_req
    
    def unpublish_object(self, id):
        endpoint = f'qrs/app/object/{id}/unpublish'
        publish_req = self.session.put(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}', verify=False, headers= self.NTLM_HEADERS )
        return publish_req
    
    def approve_object(self, id):
        endpoint = f'qrs/app/object/{id}/approve'
        approve_req = self.session.post(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}', verify=False, headers= self.NTLM_HEADERS )
        return approve_req
        
    def unapprove_object(self, id):
        endpoint = f'qrs/app/object/{id}/unapprove'
        approve_req = self.session.post(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}', verify=False, headers= self.NTLM_HEADERS )
        return approve_req
    
    
    def custom_get(self, endpoint):
        req = self.session.get(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}', verify=False, headers= self.NTLM_HEADERS )
        return req
    
    def custom_post(self, endpoint, data):
        req = self.session.post(f'{self.url}/{endpoint}?{self.URL_XREF_POSTFIX}',data=json.dumps(data) ,verify=False, headers= self.NTLM_HEADERS )
        return req
    
    
    #engine funcitons
    
    def create_app(self, app_name):
        request = json.dumps({
              "jsonrpc": "2.0",
              "id": 0,
              "method": "CreateApp",
              "handle": -1,
              "params": [
                app_name
              ]
            })
        
        self._engineSocket.send(request)
        #res = self._engineSocket.recv()
        print (res)
        return res
    
    
    def get_doc_list(self):
        request= json.dumps({
            "handle": -1,
            "method": "GetDocList",
            "params": [],
            "outKey": -1,
            "id": 1
            })
        
        self._engineSocket.send(request)
    
    def  open_app(self, app_id):
        request= json.dumps({
            "method": "OpenDoc",
            "handle": -1,
            "params": [app_id],
            "outKey": -1,
            "id": 2
        }
        )
        
        self._engineSocket.send(request)
        
    
    def delete_variable(self, var_name):
        request= json.dumps({
                "handle": 1,
                "method": "DestroyVariableByName",
                "params": [f"{var_name}"],
                "outKey": -1,
                "id": 3
            })
        
        self._engineSocket.send(request)
        #res =  self._engineSocket.recv()
        print (f'variable {var_name} deleted')
        #return res
    
