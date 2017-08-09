#!/usr/bin/env python2
# -*- coding: UTF-8 -*-


import xml.etree.cElementTree as ET
import json
import re
import codecs
from unicodedata import normalize
from collections import defaultdict
import sys
import string
from string import digits

#Padrao utilizado eh o formato 00000-000
padrao_cep =re.compile(r'(\d*){5}-(\d*){3}')

#padroes de tipos de ruas identificados e tratados
padrao_ruas_re = re.compile(r'\S+', re.IGNORECASE)
tipos_ruas_padrao = ["rua", "avenida", "largo", "viaduto", "alameda", "travessa", "vila", "ladeira", "praca"]
tipos_ruas = defaultdict(set)

#ajusta todos tos tipos para o mesmo padrão
mapeamentos = { "r.": "Rua",
            "av.": "Avenida",
            }


dados = []
arquivo_json = None

#verifica se o atributo existe no elemento
def testa_atributo(element, p_atributo):
    if p_atributo not in element.attrib:
        return False
    else:
        return True


#remove a acentuação da string
def remover_acentos(txt):
    return normalize('NFKD', unicode(txt)).encode('ASCII','ignore')

#verifica se o elemento tag é endereço
def eh_endereco(element):
    if testa_atributo(element, 'k'):
        return(element.attrib['k'].startswith('addr:'))
    else:
        return False;

#verifica se o elemento tab é uma rua
def eh_rua(element):
    if testa_atributo(element, 'k'):
        return(element.attrib['k'].startswith('addr:street'))
    else:
        return False;

#valida ceps, se não estiver formatado, formata
#so foram encontrados ceps numéricos, entao coloco no formato
def eh_cep_valido(cep):
    m = padrao_cep.match(cep)
    if m == None:
        return cep[:5]+'-'+cep[5:]
    else:
        return cep

#busca encontrar no mapeamento a sigla referente
def ajusta_rua(tipo_rua):
    tipo_rua_l = tipo_rua.lower()
    if tipo_rua_l in mapeamentos:
        return  mapeamentos[tipo_rua_l]
    else:
        return tipo_rua


#verifica se o tipo de rua possui um nome padrão
def verifica_tipo_rua(nome_rua):
    m = padrao_ruas_re.search(nome_rua)
    if m:
        tipo_rua = m.group()
        
        #verifica se esta entre os tipos padrão
        if tipo_rua.lower() not in tipos_ruas_padrao:
            #tenta realizar o ajusto pelo mapeamento
            tipo_rua_n = ajusta_rua(tipo_rua)

            #verifica novamente
            if tipo_rua_n.lower() not in tipos_ruas_padrao:
                tipos_ruas[tipo_rua].add(nome_rua)
                #Caso não esteja nos tipos padrão, seta como Rua.
                return 'Rua ' + nome_rua
            else:
                return nome_rua.replace(tipo_rua, tipo_rua_n)    
        else:
            return nome_rua

#a cidade, algumas estão em maiúsculas outras em minusculas
# remover numeros do nome da cidade
def trata_cidade(nome):
    nome_t = string.capwords(nome)
    nome_t = nome_t.translate(None, digits)
    


def verifica_elemento(element):
    node = {}
    if element.tag == "node" or element.tag == "way" :
        
        
        node["id"] =  element.attrib['id']
        node["type"] =  element.tag
        
        if testa_atributo(element, 'visible'):
            node["visible"] =  element.attrib['visible']
        
        created = {}
        created["version"] = element.attrib['version']
        created["changeset"] = element.attrib['changeset']
        created["timestamp"] = element.attrib['timestamp']
        created["user"] = element.attrib['user']
        created["uid"] = element.attrib['uid']
        node["created"] =  created
        
        if testa_atributo(element, 'lat'):
            pos = []
            pos.append(float(element.attrib['lat']))
            pos.append(float(element.attrib['lon']))
            node["pos"] = pos
        
        address = {}
        node_refs = []
        #itera entre as tags que fazem parte do node ou way
        for tag in element.iter("tag"):

            #verifica se é endereço
            # se for divide em tags menores
            if eh_endereco(tag):
                add = tag.attrib['k'].split(':')

                #pega so os atributos que são endereço
                if len(add) < 3:
                    address[add[1]] = remover_acentos(tag.attrib['v'])
                   
                    #avalia os tipos de ruas
                    if eh_rua(tag):
                        address[add[1]] = verifica_tipo_rua( address[add[1]])

                    #validaçao dos ceps
                    if tag.attrib['k'].startswith('addr:postcod'):
                        address[add[1]] = eh_cep_valido(address[add[1]])

                    #tratamento da cidade
                    if tag.attrib['k'].startswith('addr:city'):
                        address[add[1]] = trata_cidade( address[add[1]] )

            #senão grava como uma tag comum
            else:
                if testa_atributo(tag, 'v'):
                    node[tag.attrib['k']] = remover_acentos(tag.attrib['v'])
          
        for ref in element.iter('nd'):          
            if testa_atributo(ref, 'ref'):
                node_refs.append(ref.attrib['ref'])
        
        if len(address) > 0:
            node['address'] = address
            #print node
        
        if len(node_refs)> 0:
            node['node_refs'] = node_refs
        
        return node
    else:
        return None


#trata o osm e converte em json, indentado ou não
#o tratamento individual será feito pelo verifica_elemento(elemento)
def processa_mapa(arquivo, identar = False):
    arquivo_json = "saida_sp.json"
    data = []
    i = 0
    with codecs.open(arquivo_json, "w") as fo:
        
        contexto = ET.iterparse(arquivo, events=("start",))
        for _, element in contexto:
            el = verifica_elemento(element)
            if el:
                data.append(el)
                if identar:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
            element.clear()
        del contexto
    return data



def processa():
    print "Início Processamento"
    print "--------------------"
    
    dados = processa_mapa('sao_paulo.osm', True)

    print "Fim"
    print "--------------------"
    

    

if __name__ == "__main__":
    processa()
