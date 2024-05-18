import json
from nltk import root_semrep
from nltk.stem.snowball import SnowballStemmer
import os
import re
import sys
import math
from pathlib import Path
from typing import Optional, List, Union, Dict
import pickle

class SAR_Indexer:
    """
    Prototipo de la clase para realizar la indexacion y la recuperacion de artículos de Wikipedia
        
        Preparada para todas las ampliaciones:
          parentesis + multiples indices + posicionales + stemming + permuterm

    Se deben completar los metodos que se indica.
    Se pueden añadir nuevas variables y nuevos metodos
    Los metodos que se añadan se deberan documentar en el codigo y explicar en la memoria
    """

    # lista de campos, el booleano indica si se debe tokenizar el campo
    # NECESARIO PARA LA AMPLIACION MULTIFIELD
    fields = [
        ("all", True), ("title", True), ("summary", True), ("section-name", True), ('url', False),
    ]
    def_field = 'all'
    PAR_MARK = '%'
    # numero maximo de documento a mostrar cuando self.show_all es False
    SHOW_MAX = 10

    all_atribs = ['urls', 'index', 'sindex', 'ptindex', 'docs', 'weight', 'articles',
                  'tokenizer', 'stemmer', 'show_all', 'use_stemming']

    

    def __init__(self):
        """
        Constructor de la classe SAR_Indexer.
        NECESARIO PARA LA VERSION MINIMA

        Incluye todas las variables necesaria para todas las ampliaciones.
        Puedes añadir más variables si las necesitas 

        """
        self.urls = set() # hash para las urls procesadas,
        self.index = {} # hash para el indice invertido de terminos --> clave: termino, valor: posting list
        self.sindex = {} # hash para el indice invertido de stems --> clave: stem, valor: lista con los terminos que tienen ese stem
        self.ptindex = {} # hash para el indice permuterm.
        self.docs = {} # diccionario de terminos --> clave: entero(docid),  valor: ruta del fichero.
        self.weight = {} # hash de terminos para el pesado, ranking de resultados.
        self.articles = {} # hash de articulos --> clave entero (artid), valor: la info necesaria para diferencia los artículos dentro de su fichero
        self.tokenizer = re.compile("\W+") # expresion regular para hacer la tokenizacion
        self.stemmer = SnowballStemmer('spanish') # stemmer en castellano
        self.show_all = False # valor por defecto, se cambia con self.set_showall()
        self.show_snippet = False # valor por defecto, se cambia con self.set_snippet()
        self.use_stemming = False # valor por defecto, se cambia con self.set_stemming()
        self.use_ranking = False  # valor por defecto, se cambia con self.set_ranking()


    ###############################
    ###                         ###
    ###      CONFIGURACION      ###
    ###                         ###
    ###############################


    def set_showall(self, v:bool):
        """

        Cambia el modo de mostrar los resultados.
        
        input: "v" booleano.

        UTIL PARA TODAS LAS VERSIONES

        si self.show_all es True se mostraran todos los resultados el lugar de un maximo de self.SHOW_MAX, no aplicable a la opcion -C

        """
        self.show_all = v


    def set_snippet(self, v:bool):
        """

        Cambia el modo de mostrar snippet.
        
        input: "v" booleano.

        UTIL PARA TODAS LAS VERSIONES

        si self.show_snippet es True se mostrara un snippet de cada noticia, no aplicable a la opcion -C

        """
        self.show_snippet = v


    #############################################
    ###                                       ###
    ###      CARGA Y GUARDADO DEL INDICE      ###
    ###                                       ###
    #############################################


    def save_info(self, filename:str):
        """
        Guarda la información del índice en un fichero en formato binario
        
        """
        info = [self.all_atribs] + [getattr(self, atr) for atr in self.all_atribs]
        with open(filename, 'wb') as fh:
            pickle.dump(info, fh)

    def load_info(self, filename:str):
        """
        Carga la información del índice desde un fichero en formato binario
        
        """
        #info = [self.all_atribs] + [getattr(self, atr) for atr in self.all_atribs]
        with open(filename, 'rb') as fh:
            info = pickle.load(fh)
        atrs = info[0]
        for name, val in zip(atrs, info[1:]):
            setattr(self, name, val)

    ###############################
    ###                         ###
    ###   PARTE 1: INDEXACION   ###
    ###                         ###
    ###############################

    def already_in_index(self, article:Dict) -> bool:
        """

        Args:
            article (Dict): diccionario con la información de un artículo

        Returns:
            bool: True si el artículo ya está indexado, False en caso contrario
        """
        return article['url'] in self.urls


    def index_dir(self, root:str, **args):
        """
        
        Recorre recursivamente el directorio o fichero "root" 
        NECESARIO PARA TODAS LAS VERSIONES
        
        Recorre recursivamente el directorio "root"  y indexa su contenido
        los argumentos adicionales "**args" solo son necesarios para las funcionalidades ampliadas

        """
        self.multifield = args['multifield']
        self.positional = args['positional']
        self.stemming = args['stem']
        self.permuterm = args['permuterm']

        file_or_dir = Path(root)
        
        if file_or_dir.is_file():
            # is a file
            self.index_file(root)
        elif file_or_dir.is_dir():
            # is a directory
            for d, _, files in os.walk(root):
                for filename in sorted(files):
                    if filename.endswith('.json'):
                        fullname = os.path.join(d, filename)
                        self.index_file(fullname)
        else:
            print(f"ERROR:{root} is not a file nor directory!", file=sys.stderr)
            sys.exit(-1)

        ##########################################
        ## COMPLETAR PARA FUNCIONALIDADES EXTRA ##
        ##########################################

        #si esta activado el uso de stemming llamamos a make_stemming para rellenar sel.sindex
        if self.stemming:
            self.make_stemming()
        
        
    def parse_article(self, raw_line:str) -> Dict[str, str]:
        """
        Crea un diccionario a partir de una linea que representa un artículo del crawler

        Args:
            raw_line: una linea del fichero generado por el crawler

        Returns:
            Dict[str, str]: claves: 'url', 'title', 'summary', 'all', 'section-name'
        """
        
        article = json.loads(raw_line)
        sec_names = []
        txt_secs = ''
        for sec in article['sections']:
            txt_secs += sec['name'] + '\n' + sec['text'] + '\n'
            txt_secs += '\n'.join(subsec['name'] + '\n' + subsec['text'] + '\n' for subsec in sec['subsections']) + '\n\n'
            sec_names.append(sec['name'])
            sec_names.extend(subsec['name'] for subsec in sec['subsections'])
        article.pop('sections') # no la necesitamos 
        article['all'] = article['title'] + '\n\n' + article['summary'] + '\n\n' + txt_secs
        article['section-name'] = '\n'.join(sec_names)

        return article
                
    
    def index_file(self, filename:str): #Luis José Ferrer Estellés
        """

        Indexa el contenido de un fichero.
        
        input: "filename" es el nombre de un fichero generado por el Crawler cada línea es un objeto json
            con la información de un artículo de la Wikipedia

        NECESARIO PARA TODAS LAS VERSIONES

        dependiendo del valor de self.multifield y self.positional se debe ampliar el indexado


        """

        #comprobamos si self.docs esta vacio , si lo esta lo inicializamos y si no esta vacio le ponemos como id la cantidad de documentos más 1 
        documents= list(self.docs.items())
        if(documents==[]):
            self.docs[1] = filename
            docid=1
        else:
            length = len(documents)
            self.docs[length+1] = filename
            docid=length+1
        
        #creamos un indice para cada sección del articulo si no existe ya
        
        if self.multifield:
            for field in self.fields:
                if self.index.get(field[0]) is None:
                    self.index[field[0]] = {}
        
        for i, line in enumerate(open(filename)):
            j = self.parse_article(line)
        #
        # 
        # En la version basica solo se debe indexar el contenido "article"
        #
        #
        #
        #################
        ### COMPLETADO ###
        #################

            #si el articulo ya esta indexado, no lo indexamos de nuevo
            if self.already_in_index(j):
                continue
            #sacamos el id para la clave articulo y guardamos en su valor una tupla de docid y la posicion del articulo en el fichero
            artic= list(self.articles.items())
            artid=len(artic)+1
            self.articles[artid] =(docid,i)
            if self.multifield:
                #iteramos sobre field para ver que campos tenemos que tokenizar, dentro de los que hay que tokenizar iteramos sobre los tokens y los guardamos en el indice
               
                for tupla in self.fields:
                    if tupla[1]:
                        for token in self.tokenize(j[tupla[0]]):
                            if self.index[tupla[0]].get(token) is None:
                                self.index[tupla[0]][token] = [artid]
                            elif artid not in self.index[tupla[0]][token]:
                                self.index[tupla[0]][token].append(artid)
                    else:
                        self.index[tupla[0]] = j[tupla[0]]           
               
            else:        
              txt = j['all']
              tokens_list = self.tokenize(txt)
              for token in tokens_list:
                  if self.index.get(token) is None:
                      self.index[token] = [artid]
                  elif self.index[token].count(artid) == 0:
                      self.index[token].append(artid)
            self.urls.add(j['url'])    
    

    def set_stemming(self, v:bool):
        """

        Cambia el modo de stemming por defecto.
        
        input: "v" booleano.

        UTIL PARA LA VERSION CON STEMMING

        si self.use_stemming es True las consultas se resolveran aplicando stemming por defecto.

        """
        self.use_stemming = v


    def tokenize(self, text:str):
        """
        NECESARIO PARA TODAS LAS VERSIONES

        Tokeniza la cadena "texto" eliminando simbolos no alfanumericos y dividientola por espacios.
        Puedes utilizar la expresion regular 'self.tokenizer'.

        params: 'text': texto a tokenizar

        return: lista de tokens

        """
        return self.tokenizer.sub(' ', text.lower()).split()


    def make_stemming(self):
        """

        Crea el indice de stemming (self.sindex) para los terminos de todos los indices.

        NECESARIO PARA LA AMPLIACION DE STEMMING.

        "self.stemmer.stem(token) devuelve el stem del token"


        """
        
        ####################################################
        ## COMPLETAR PARA FUNCIONALIDAD EXTRA DE STEMMING ##
        ####################################################
        if self.multifield:
            for tupla in self.fields:
                self.sindex[tupla[0]] = {}
                for token in self.index[tupla[0]]:
                    stem = self.stemmer.stem(token)
                    if self.sindex[tupla[0]].get(stem) is None:
                        postinglist = self.get_stemming(token, field=None)
                    else:
                        postinglist = self.get_stemming(token, field=self.sindex[tupla[0]][stem])
                    self.sindex[tupla[0]][stem] = postinglist
                    
                
        else:
            for token in self.index:
                stem = self.stemmer.stem(token)
                if self.sindex.get(stem) is None:
                    postinglist = self.get_stemming(token, field=None)
                else:
                    postinglist = self.get_stemming(token, field=self.sindex[stem])
                self.sindex[stem] = postinglist
        



    
    def make_permuterm(self):
        """

        Crea el indice permuterm (self.ptindex) para los terminos de todos los indices.

        NECESARIO PARA LA AMPLIACION DE PERMUTERM


        """
        pass
        ####################################################
        ## COMPLETAR PARA FUNCIONALIDAD EXTRA DE STEMMING ##
        ####################################################




    def show_stats(self):#Luis José Ferrer Estellés
        """
        NECESARIO PARA TODAS LAS VERSIONES
        
        Muestra estadisticas de los indices
        
        """

        ########################################
        ## COMPLETADO PARA TODAS LAS VERSIONES ##
        ########################################
        print("========================================")
        print("Number of indexed files: " + str(len(self.docs)))
        print("----------------------------------------")
        print("Number of indexed articles: " + str(len(self.articles)))
        print("----------------------------------------")
        print("TOKENS")

        if self.multifield:
            for field in self.fields:
                print("# of tokens in '" + field[0] + "': " + str(len(self.index[field[0]])))
        else:
            print("# of tokens: " + str(len(self.index)))
        if self.stemming:
            print("----------------------------------------")
            print("STEMS")
            if self.multifield:
                for field in self.fields:
                    print("# of stems in '" + field[0] + "': " + str(len(self.sindex[field[0]])))
            else:
                print("# of stems: " + str(len(self.sindex)))

    #################################
    ###                           ###
    ###   PARTE 2: RECUPERACION   ###
    ###                           ###
    #################################

    ###################################
    ###                             ###
    ###   PARTE 2.1: RECUPERACION   ###
    ###                             ###
    ###################################


    def solve_query(self, query:str, prev:Dict={}): #Ricardo Díaz y David Oltra
        """
        NECESARIO PARA TODAS LAS VERSIONES

        Resuelve una query.
        Debe realizar el parsing de consulta que sera mas o menos complicado en funcion de la ampliacion que se implementen


        param:  "query": cadena con la query
                "prev": incluido por si se quiere hacer una version recursiva. No es necesario utilizarlo.


        return: posting list con el resultado de la query

        """

        if query is None or len(query) == 0:
            return []
        if isinstance(query, str):
            tokens = self.tokenize(query)   #tokenizamos la query
        else:
            tokens = query
        if len(tokens) == 1:  #si solo hay un token en la query
            term, field = self.get_field(tokens[0])  #obtenemos el token y el campo
            return self.get_posting(term, field)  #devolvemos la posting list del token
        else:
            opi = len(tokens) - 2  #el penúltimo token de la query es un operador
            op = tokens[opi]
            preop = tokens[:opi]  #los tokens anteriores al operador
            postop = tokens[opi+1:] #los tokens posteriores al operador
            if op == 'not':
                if opi == 0: #el NOT está al principio de la query
                    return self.reverse_posting(self.solve_query(postop))  #devolvemos la NOT del resto (sea un token o una query)
                else:
                    opi -= 1
                    op = tokens[opi]
                    preop = tokens[:opi]
                    if op == 'and':
                        return self.minus_posting(self.solve_query(preop), self.solve_query(postop))  #devolvemos la resta de las dos posting list
                    elif op == 'or':
                        return self.or_posting(self.solve_query(preop), self.reverse_posting(self.solve_query(postop)))
            elif op == 'and':
                return self.and_posting(self.solve_query(preop), self.solve_query(postop))
            elif op == 'or':
                return self.or_posting(self.solve_query(preop), self.solve_query(postop))

        ########################################
        ## COMPLETAR PARA TODAS LAS VERSIONES ##
        ########################################

    def get_field(self, cadena:str): #Ricardo Díaz y David Oltra
        tokens = cadena.split(':')
        if len(tokens) == 1:
            return tokens[0], self.def_field
        else:
            return tokens[1], tokens[0]
        
    def get_posting(self, term:str, field:Optional[str]=None): #Ricardo Díaz y David Oltra
        """

        Devuelve la posting list asociada a un termino. 
        Dependiendo de las ampliaciones implementadas "get_posting" puede llamar a:
            - self.get_positionals: para la ampliacion de posicionales
            - self.get_permuterm: para la ampliacion de permuterms
            - self.get_stemming: para la amplaicion de stemming


        param:  "term": termino del que se debe recuperar la posting list.
                "field": campo sobre el que se debe recuperar la posting list, solo necesario si se hace la ampliacion de multiples indices

        return: posting list
        
        NECESARIO PARA TODAS LAS VERSIONES

        """

        ########################################
        ## COMPLETAR PARA TODAS LAS VERSIONES ##
        ########################################

        pl = self.index.get(term)
        return pl



    def get_positionals(self, terms:str, index):
        """

        Devuelve la posting list asociada a una secuencia de terminos consecutivos.
        NECESARIO PARA LA AMPLIACION DE POSICIONALES

        param:  "terms": lista con los terminos consecutivos para recuperar la posting list.
                "field": campo sobre el que se debe recuperar la posting list, solo necesario se se hace la ampliacion de multiples indices

        return: posting list

        """
        pass
        ########################################################
        ## COMPLETAR PARA FUNCIONALIDAD EXTRA DE POSICIONALES ##
        ########################################################


    def get_stemming(self, term:str, field: Optional[str]=None):
        """

        Devuelve la posting list asociada al stem de un termino.
        NECESARIO PARA LA AMPLIACION DE STEMMING

        param:  "term": termino para recuperar la posting list de su stem.
                "field": campo sobre el que se debe recuperar la posting list, solo necesario se se hace la ampliacion de multiples indices

        return: posting list

        """
        
        stem = self.stemmer.stem(term)
 
        ####################################################
        ## COMPLETAR PARA FUNCIONALIDAD EXTRA DE STEMMING ##
        ####################################################
        

        #inicializamos la posting list con term si esta vacia
        if field is None:
            field = [term]
            
        
        #si el termino no esta en la posting list, lo añadimos
        if term not in field:
            field.append(term)
        
        return field        

    def get_permuterm(self, term:str, field:Optional[str]=None):
        """

        Devuelve la posting list asociada a un termino utilizando el indice permuterm.
        NECESARIO PARA LA AMPLIACION DE PERMUTERM

        param:  "term": termino para recuperar la posting list, "term" incluye un comodin (* o ?).
                "field": campo sobre el que se debe recuperar la posting list, solo necesario se se hace la ampliacion de multiples indices

        return: posting list

        """

        ##################################################
        ## COMPLETAR PARA FUNCIONALIDAD EXTRA PERMUTERM ##
        ##################################################
        pass



    def reverse_posting(self, p): #Ricardo Díaz y David Oltra
        """
        Devuelve una posting list con todas las noticias excepto las contenidas en p.
        Util para resolver las queries con NOT.

        param:  "p": posting list

        return: posting list con todos los newid exceptos los contenidos en p
        """
        # Obtener todos los documentos en el índice
        all_arts = list(self.articles.keys())

        # Utilizar el método minus_posting para obtener todos los documentos excepto los que están en p
        return self.minus_posting(all_arts, p)
                                  
        ########################################
        ## COMPLETAR PARA TODAS LAS VERSIONES ##
        ########################################



    def and_posting(self, p1:list, p2:list): #Diana Bachynska
        """
        NECESARIO PARA TODAS LAS VERSIONES

        Calcula el AND de dos posting list de forma EFICIENTE

        param:  "p1", "p2": posting lists sobre las que calcular


        return: posting list con los artid incluidos en p1 y p2

        """
        
        res  = []
        i1 = 0
        i2 = 0

        if p1 == [] or p2 == []:  #si las dos posting list están vacias, devuelvo una lista vacía
            return []
        
        while i1 < len(p1) and i2 < len(p2): #mientras no llegue al final de p1 y al final de p2
            if  p1[i1] == p2[i2]:  #si p1 y p2 contienen el mismo documento
                res.append(p1[i1]) #añado la re
                i1 += 1
                i2 += 1          
            elif p1[i1]  < p2[i2]:
                i1 += 1            
            else: i2 += 1

        return res


    def or_posting(self, p1:list, p2:list): #Diana Bachynska
        """
        NECESARIO PARA TODAS LAS VERSIONES

        Calcula el OR de dos posting list de forma EFICIENTE

        param:  "p1", "p2": posting lists sobre las que calcular


        return: posting list con los artid incluidos de p1 o p2

        """

        res  = []
        i1 = 0
        i2 = 0

        if p1 == [] or p2 == []:  #si las dos posting list están vacias, devuelvo una lista vacía
            return []
        
        while i1 < len(p1) and i2 < len(p2): #mientras no llegue al final de p1 y al final de p2
            if  p1[i1] == p2[i2]:  #si p1 y p2 contienen el mismo documento
                res.append(p1[i1]) 
                i1 += 1
                i2 += 1          
            elif p1[i1]  < p2[i2]:
                res.append(p1[i1]) 
                i1 += 1 

            else:
                res.append(p2[i2]) 
                i2 += 1 
        
        while i1 < len(p1):
            res.append(p1[i1])
            i1 += 1 
        
        while i2 < len(p2):
            res.append(p2[i2]) 
            i2 += 1 
        
        return res


    def minus_posting(self, p1, p2): #Diana Bachynska
        """
        OPCIONAL PARA TODAS LAS VERSIONES

        Calcula el except de dos posting list de forma EFICIENTE.
        Esta funcion se incluye por si es util, no es necesario utilizarla.

        param:  "p1", "p2": posting lists sobre las que calcular


        return: posting list con los artid incluidos de p1 y no en p2

        """

        res  = []
        i1 = 0
        i2 = 0

        while i1 < len(p1) and i2 < len(p2): #mientras no llegue al final de p1 y al final de p2
            if  p1[i1] == p2[i2]:  #si p1 y p2 contienen el mismo documento
                i1 += 1
                i2 += 1          
            elif p1[i1]  < p2[i2]:
                res.append(p1[i1]) #añado a res el documento i1 de p1
                i1 += 1            
            else: i2 += 1

        while i1 < len(p1):
            res.append(p1[i1])
            i1 += 1 
        
        return res

    #####################################
    ###                               ###
    ### PARTE 2.2: MOSTRAR RESULTADOS ###
    ###                               ###
    #####################################

    def solve_and_count(self, ql:List[str], verbose:bool=True) -> List:
        results = []
        for query in ql:
            if len(query) > 0 and query[0] != '#':
                r = self.solve_query(query)
                results.append(len(r))
                if verbose:
                    print(f'{query}\t{len(r)}')
            else:
                results.append(0)
                if verbose:
                    print(query)
        return results


    def solve_and_test(self, ql:List[str]) -> bool:
        errors = False
        for line in ql:
            if len(line) > 0 and line[0] != '#':
                query, ref = line.split('\t')
                reference = int(ref)
                result = len(self.solve_query(query))
                if reference == result:
                    print(f'{query}\t{result}')
                else:
                    print(f'>>>>{query}\t{reference} != {result}<<<<')
                    errors = True                    
        return not errors


    def solve_and_show(self, query:str): #Ricardo Díaz y David Oltra
        """
        NECESARIO PARA TODAS LAS VERSIONES

        Resuelve una consulta y la muestra junto al numero de resultados 

        param:  "query": query que se debe resolver.

        return: el numero de artículo recuperadas, para la opcion -T

        """

        sol = self.solve_query(query)
        print(sol)
        print("========================================")
        i = 1
        for artid in sol:
            docid, linea = self.articles[artid]
            doc = self.docs[docid]
            with open(doc) as fh:
                dic = self.parse_article(fh.readlines()[linea])
            print(f"# {i:02d} {dic['title']}: {dic['url']}")
            i+=1
        print("========================================")
        print(f"Number of results: {len(sol)}")
        return len(sol)
        ################
        ## COMPLETAR  ##
        ################
        






        

