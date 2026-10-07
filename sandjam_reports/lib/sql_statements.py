"""Split SQL safely, ignoring comments and separators inside quoted literals."""
def split_sql(sql):
    result,buf=[],[]
    i=0
    quote=None
    while i<len(sql):
        c=sql[i]
        if quote:
            buf.append(c)
            if c=='\\' and i+1<len(sql):
                i+=1
                buf.append(sql[i])
            elif c==quote:
                if i+1<len(sql) and sql[i+1]==quote:
                    i+=1
                    buf.append(sql[i])
                else:
                    quote=None
        elif sql.startswith('--',i):
            end=sql.find('\n',i)
            i=len(sql) if end<0 else end
            buf.append('\n')
            continue
        elif sql.startswith('/*',i):
            end=sql.find('*/',i+2)
            if end<0:
                raise ValueError('Unclosed SQL block comment')
            i=end+2
            buf.append(' ')
            continue
        elif c in ("'",'"','`'):
            quote=c
            buf.append(c)
        elif c==';':
            statement=''.join(buf).strip()
            if statement:
                result.append(statement)
            buf=[]
        else:
            buf.append(c)
        i+=1
    if quote:
        raise ValueError('Unclosed SQL quote')
    statement=''.join(buf).strip()
    if statement:
        result.append(statement)
    return result
