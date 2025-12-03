

// get the incidents caused by a different CI_SC than the CI_SC that is affected by the interaction - built statistics
MATCH (c1:CI_SC)<-[:AFFECTED_CI_SC]-(int:Interaction)-[:RELATED_INCIDENT]->(inc:Incident)-[:CAUSED_BY_CI_SC]->(c2:CI_SC) WHERE c1 <> c2
WITH c1,c2
MATCH (c1)<-[:AFFECTED_CI_SC]-(ch1:Change)
WITH c1,c2,count(ch1) AS ch1_count
MATCH (c2)<-[:AFFECTED_CI_SC]-(ch2:Change)
RETURN c1.sysId,c2.sysId, ch1_count,count(ch2) AS ch2_count

// get the graph of a specific incident and interaction with affected/causing CI_SCs
MATCH (c1:CI_SC {sysId:"APP000003_WBS000098"})<-[r1:AFFECTED_CI_SC]-(int:Interaction)-[r2:RELATED_INCIDENT]->(inc:Incident)-[r3:CAUSED_BY_CI_SC]->(c2:CI_SC) WHERE c1 <> c2
MATCH (c1)<-[r4:AFFECTED_CI_SC]-(ch1:Change)
MATCH (c2)<-[r5:AFFECTED_CI_SC]-(ch2:Change)
RETURN c1,int,inc,c2,ch1,ch2,r1,r2,r3,r4,r5

// get the timeline of an entire CI_SC (including DFs of parallel objects)
MATCH (ci_sc:CI_SC {sysId:"WBS000098_APP000003"})<-[:CORR]-(e:Event) -[df:DF]-> (e2:Event)-[:CORR]->(ci_sc)
RETURN e,df,e2

WBS000253_ADB000028