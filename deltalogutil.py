import json, boto3, os, sys,re

def getClient(type, profile):
    if not profile is None:
        session = boto3.Session(profile_name=profile)
        return session.client(type)
    else:
        return boto3.client(type)

s3Client = getClient('s3',os.getenv('AWS_PROFILE', default=None))

def generateNewSymLink(path):
    if 's3' in path:  
        sBucket,sPefix= splitS3Path(path)
        files, partition_files = parseDeltaLog(path)
        if len(files)>0:
            symPefix = sPefix + '_symlink/symlink.txt'
            print('final_files',len(files))
            files = [path+ f for f in  files]
            writeSymLink(sBucket, symPefix,  files)
            print(path+"_symlink/", len(files))
        elif len(partition_files)>0:   
            for key,d_files in partition_files.iteritems():
                print(path+"_symlink/"+key, len(d_files))
                symPefix = sPefix + '_symlink/'+key+'/symlink.txt'
                files = [path+ f for f in  d_files]
                writeSymLink(sBucket, symPefix,  files)

def writeSymLink(sBucket, symPefix, files):
    content = "\n".join(files)
    #print(symPefix, files)
    s3Client.put_object(Bucket=sBucket, Key=key, Body=content)

def parseDeltaLog(path):

    files,partition_files = [], {}
    
    sBucket,sPefix= splitS3Path(path)
    prefix = (sPefix if sPefix.endswith("/") else sPefix+"/") + "_delta_log/"
    json_files = getFiles(sBucket,prefix)

    for j_file in json_files['files']:
        j_bucket, j_key = splitS3Path(j_file)
        #print('j_bucket',j_bucket,'j_key',j_key)
        content = s3Client.get_object(Bucket=j_bucket,Key=j_key)['Body'].read()
        for line in content.splitlines():
            try:
                ope = None
                if "{\"add\"" in line: ope = "add"
                elif "{\"remove\"" in line: ope = "remove"
                #print('ope',ope,line)
                if ope is not None and "\"path\"" in line:
                    d_add_json = json.loads(line)
                    #partitionValues = d_add_json['partitionValues']
                    d_add_path = str(d_add_json[ope]["path"])
                    if '/part' in d_add_path:
                        pKeys = d_add_path[:d_add_path.index("/part")]
                        if pKeys not in partition_files: partition_files[pKeys]=[]
                        if ope=="add":partition_files[pKeys].append(d_add_path)
                        elif ope=="remove":partition_files[pKeys].remove(d_add_path)
                    else:
                        if ope=="add": files.append(d_add_path)
                        elif ope=="remove":files.remove(d_add_path)
            except Exception as e:
                print("*ERR*",str(e),line)
    return files,partition_files 

def splitS3Path(s3_bucket_path):
    path_parts = s3_bucket_path.replace("s3://","").split("/")
    bucket =  path_parts.pop(0)
    key = "/".join(path_parts)
    return bucket, key 

def getFiles(bucket,prefix):
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    files, size = [], 0
    while True:
        resp = s3Client.list_objects_v2(**kwargs)
        #print('< s3 resp>',resp)
        if not resp:
            print('no response found',resp)
            break
        if not 'Contents' in resp:
            print('No files found')
            break
        for obj in resp['Contents']:
            size = size + obj['Size']
            isInclude = True
            if exclude is not None and re.match(exclude, obj['Key']):
                isInclude = False
            if include is not None and len(include)>0:
                if not re.match(include, obj['Key']): 
                    isInclude = False
            if not obj['Key'].endswith("/") and isInclude: files.append("s3://"+bucket+"/"+obj['Key'])
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break
    return {'files':files,'size':size}