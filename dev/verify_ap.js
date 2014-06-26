/*
 * Verfify results from
 * php -f /var/www/crm/src/Dappl/src/Dappl/dev/graph.php People 500 "PersonCentreRoles/Centre/PartnerType eq 'Access Point'" PersonID,Email
 *
 * Get all people attached to centres with PartnerType = "Access Point"
 *
 * Report results are not unique so people maybe listed multiple times, if they are attached to multiple centres.
 *
 * Method:
 *
 * Fetch from People: all PersonID
 * Fetch from PeopleLinksCentresRoles: CentreID for those PersonID
 * Fetch from Centre: CentreID with PartnerType eq 'Access Point'
 *
 *
 *
 */
var conn = new Mongo();
var uk = conn.getDB("ukonline");
var ns = conn.getDB("NetsuiteTranslated");
var personIDs = [];
var centreIDs = [];
var results = [];
var doc;
var peopleCursor, pcrCursor, centreCursor, resultCount;


// Get PersonID list from People
peopleCursor = ns.People.find();
while (peopleCursor.hasNext()) {
    doc = peopleCursor.next();
    personIDs.push(doc.PersonID);
}
print('PersonID list from People: ' + personIDs.length);


// Get CentreID list from PersonCentreRoles
pcrCursor = ns.PeopleLinksCentresRoles.find({PersonID: { $in : personIDs}});
while (pcrCursor.hasNext()) {
    doc = pcrCursor.next();
    centreIDs.push(doc.CentreID);
}
print('CentreID list from PeopleLinksCentresRoles: ' + centreIDs.length);


// Get final count from centres
centreCursor = uk.Centres.find({PartnerType: 'Access Point', CentreID: { $in: centreIDs}});
resultCount = centreCursor.count();
print('Unduped result count: ' + resultCount);


// Get unduped by walking back to the roles collection - get CentreID of access point matches
centreIDs = [];
while (centreCursor.hasNext()) {
    doc = centreCursor.next();
    centreIDs.push(doc.CentreID);
}


// Get duped count from roles
resultCount = ns.PeopleLinksCentresRoles.find({CentreID: { $in : centreIDs}}).count();
print('Unduped result count: ' + resultCount);
