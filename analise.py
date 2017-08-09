#!/usr/bin/env python2
# -*- coding: UTF-8 -*-

import pprint

def get_db():
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client.dbopen
    return db

def imprime(lista, campo_total= 'count'):
    
    for item in lista:
        if item['_id'] == None:
            print 'Nao Identificado: ' + str(item[campo_total])
        else:
            #print unicode(item['_id'])
            print unicode(item['_id']) + ': ' + str(item[campo_total]) 



def monta_analise_cep():
    pipeline = [ {"$match":{"address.postcode":{"$exists":1}}},
                     {"$group":{"_id":"$address.postcode", "count":{"$sum":1}}},
                     {"$sort": {"count":-1}},
                     {"$limit": 3}]

    return pipeline

def monta_analise_cidade():

    pipeline = [{"$match":{"address.city":{"$exists":1}}},
                {"$group":{"_id":"$address.city", "count":{"$sum":1}}},
                {"$sort": {"count":-1}},
                {"$limit": 10}]

    return pipeline

def monta_analise_usuarios(quantidade):

    pipeline = [{"$group":{"_id":"$created.user", "count":{"$sum":1}}},
                 {"$sort": {"count":-1}}]

    if quantidade > 0:
        pipeline.append({"$limit":quantidade})
     
    return pipeline

def monta_analise_usuario_uni_pub():
    pipeline =  [{"$group":{"_id":"$created.user", "count":{"$sum":1}}},
                {"$group":{"_id":"$count", "num_users":{"$sum":1}}},
                {"$sort":{"_id":1}},
            {"$limit":1}]
    return pipeline

def monta_analise_facilidades():
    pipeline =  [{"$match":{"amenity":{"$exists":1}}},
            {"$group":{"_id":"$amenity","count":{"$sum":1}}},
            {"$sort":{"count": -1}},
            {"$limit":10}]
    return pipeline


def monta_analise_religiao():
    pipeline =  [{"$match":{"amenity":{"$exists":1}, "amenity":"place_of_worship"}},
            {"$group":{"_id":"$religion", "count":{"$sum":1}}},
            {"$sort":{"count": -1}},
            {"$limit":10}]
    return pipeline

def monta_analise_cozinhas():
    pipeline =  [{"$match":{"amenity":{"$exists":1}, "amenity":"restaurant"}},
            {"$group":{"_id":"$cuisine", "count":{"$sum":1}}},
            {"$sort":{"count":-1}},
            {"$limit":10}]
    return pipeline

def monta_analise_ano():
    pipeline = [ {   "$project" : { "ano" : {"$substr": ["$created.timestamp", 0, 4] }}},
                 {   "$group"   : { "_id" : "$ano",  "count" : { "$sum" : 1 }}},
                 {   "$sort"    : { "count" : -1 }}  ]
    return pipeline

#método para agregar os dados de acordo com os pipelines passados
def aggregate(db, pipeline):
    return [doc for doc in db.sp.aggregate(pipeline)]



#trecho principal da analise
if __name__ == "__main__":
    db = get_db()

    
    tot_documentos = db.sp.find().count()

    print "Análise por ano:"
    #analise por ano
    ano = aggregate(db, monta_analise_ano())
    imprime(ano)


    print "\n CEPs com maiores pontos:"
    #analise por cep
    locais = aggregate(db, monta_analise_cep())
    imprime(locais)

    print "\nCidades com maiores pontos: "
    #analise por cidade
    locais2 = aggregate(db, monta_analise_cidade())
    imprime(locais2)

    print "\nUsuarios com maiores publicacoes: "
    #maiores publicador
    usuarios = aggregate(db, monta_analise_usuarios(10))
    imprime(usuarios)

    print "\nDistribuicao das publicaces por Usuarios %: "

    i = 1
    soma_maiores = 0
    soma_10_maiores = 0
    for usu in usuarios:
        if i <3:
             soma_maiores += usu['count']

        soma_10_maiores += usu['count']
        print usu['_id'] + ': ' + str(round(( usu['count']/float(tot_documentos))*100, 2)) + '%'
        i+=1

    print '\n Os 2 maiores representam: ' + str(round(( soma_maiores/float(tot_documentos))*100, 2)) + '%'
    print '\n Os 10 maiores representam: ' + str(round(( soma_10_maiores/float(tot_documentos))*100, 2)) + '%'

    usuariosTotoais = aggregate(db, monta_analise_usuarios(0))
    k = 0
    for usu2 in usuariosTotoais:
        if ( usu2['count']/float(tot_documentos))*100 <= 1:
            k +=1

    print '\n Total de usuarios que representam menos de 1%: ' + str(k)


    print "\nQuantidade de usuarios que contribuiram apenas 1 vez:"
    usuarios2 = aggregate(db, monta_analise_usuario_uni_pub())
    imprime(usuarios2, 'num_users')
    #pprint.pprint(usuarios2)

    print "\n Facilidades: "
    facilidades = aggregate(db, monta_analise_facilidades())
    imprime(facilidades)

    print "\n Religiao: "
    religiao = aggregate(db, monta_analise_religiao())
    imprime(religiao)

    print "\n Cozinhas: "
    cozinhas = aggregate(db, monta_analise_cozinhas())
    imprime(cozinhas)


    print "\n Numeros em geral"
    #documentos
    print "Documentos: " + str(tot_documentos )                                               

    #nós
    print "Nos: " + str(db.sp.find({"type":"node"}).count())
                                                
    #ways
    print "Ways: " + str(db.sp.find({"type":"way"}).count())
    
    
    #usuarios unicos
    print "Usuarios unicos: "+ str(len(db.sp.distinct("created.user")))
    #pprint.pprint(a)

