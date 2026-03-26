query="PREFIX rdfs:   <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dcat:   <http://www.w3.org/ns/dcat#>
PREFIX dct:    <http://purl.org/dc/terms/>
PREFIX dcv: <https://dataid.dbpedia.org/databus-cv#>
PREFIX databus: <https://dataid.dbpedia.org/databus#>
SELECT ?file WHERE
{
	{
		GRAPH ?g
		{
			{
				?dataset databus:group <https://databus.dbpedia.org/dbpedia/generic> .
				{ ?distribution <https://dataid.dbpedia.org/databus#compression> 'bzip2' . }
				{ ?distribution <https://dataid.dbpedia.org/databus#formatExtension> 'ttl' . }
				{
					?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/generic/geo-coordinates> .
					{ ?distribution <https://dataid.dbpedia.org/databus-cv#lang> 'en' . }
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/generic/geo-coordinates> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
				UNION
				{
					?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/generic/infobox-properties> .
					{ ?distribution <https://dataid.dbpedia.org/databus-cv#lang> 'en' . }
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/generic/infobox-properties> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
				UNION
				{
					?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/generic/infobox-property-definitions> .
					{ ?distribution <https://dataid.dbpedia.org/databus-cv#lang> 'en' . }
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/generic/infobox-property-definitions> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
				UNION
				{
					?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/generic/labels> .
					{ ?distribution <https://dataid.dbpedia.org/databus-cv#lang> 'en' . }
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/generic/labels> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
				UNION
				{
					?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/generic/persondata> .
					{ ?distribution <https://dataid.dbpedia.org/databus-cv#lang> 'en' . }
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/generic/persondata> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
			}
			UNION
			{
				?dataset databus:group <https://databus.dbpedia.org/dbpedia/mappings> .
				{ ?distribution <https://dataid.dbpedia.org/databus#compression> 'bzip2' . }
				{ ?distribution <https://dataid.dbpedia.org/databus-cv#lang> 'en' . }
				{
					?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/mappings/geo-coordinates-mappingbased> .
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/mappings/geo-coordinates-mappingbased> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
				UNION
				{
					?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/mappings/instance-types> .
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/mappings/instance-types> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
				UNION
				{
					?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals> .
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
				UNION
				{
					?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/mappings/mappingbased-objects> .
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/mappings/mappingbased-objects> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
				UNION
				{
					?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/mappings/specific-mappingbased-properties> .
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/mappings/specific-mappingbased-properties> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
			}
			UNION
			{
				?dataset databus:group <https://databus.dbpedia.org/dbpedia/transition> .
				{ ?distribution <https://dataid.dbpedia.org/databus-cv#lang> 'en' . }
				{
					?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/transition/freebase-links> .
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/transition/freebase-links> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
				UNION
				{
					?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/transition/links> .
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/transition/links> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
				UNION
				{
					?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/transition/sdtypes> .
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/dbpedia/transition/sdtypes> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
			}
			UNION
			{
				?dataset databus:group <https://databus.dbpedia.org/propan/lhd> .
				{
					?dataset databus:artifact <https://databus.dbpedia.org/propan/lhd/linked-hypernyms> .
					{ ?distribution <https://dataid.dbpedia.org/databus-cv#lang> 'en' . }
					{
						?distribution <https://dataid.dbpedia.org/databus-cv#type> ?c0 .
						VALUES ?c0 {
							'extension'
							'core'
						}
					}
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/propan/lhd/linked-hypernyms> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
			}
			UNION
			{
				?dataset databus:group <https://databus.dbpedia.org/ontologies/dbpedia.org> .
				{
					?dataset databus:artifact <https://databus.dbpedia.org/ontologies/dbpedia.org/ontology--DEV> .
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/ontologies/dbpedia.org/ontology--DEV> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
					{ ?distribution <https://dataid.dbpedia.org/databus#formatExtension> 'nt' . }
				}
			}
			UNION
			{
				?dataset databus:group <https://databus.dbpedia.org/vehnem/yago> .
				{
					?dataset databus:artifact <https://databus.dbpedia.org/vehnem/yago/taxonomy> .
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/vehnem/yago/taxonomy> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
				UNION
				{
					?dataset databus:artifact <https://databus.dbpedia.org/vehnem/yago/instance-types> .
					{
						?distribution dct:hasVersion ?version {
							SELECT (?v as ?version) { 
								GRAPH ?g2 { 
									?dataset databus:artifact <https://databus.dbpedia.org/vehnem/yago/instance-types> . 
									?dataset dct:hasVersion ?v . 
								}
							} ORDER BY DESC (STR(?version)) LIMIT 1 
						}
					}
				}
			}
			?dataset dcat:distribution ?distribution .
			?distribution databus:file ?file .
		}
	}
}
"
files=$(curl -X POST -H "Accept: text/csv" --data-urlencode "query=${query}" https://databus.dbpedia.org/sparql | tail -n +2 | sed 's/\r$//' | sed 's/"//g')
echo $files
rm -rf *.ttl
rm -rf *.bz2
rm -rf *.bzip2
rm -rf *.nt

while IFS= read -r file ; do wget $file; done <<< "$files"