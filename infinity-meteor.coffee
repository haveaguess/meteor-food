# Performance monitoring : https://kadira.io/
# watching DDP msgs : https://github.com/arunoda/meteor-ddp-analyzer
# DDP: Distributed Data Protocol: 
#   https://github.com/meteor/meteor/blob/devel/packages/ddp/DDP.md
#   https://meteorhacks.com/introduction-to-ddp.html
# heavy use of http://js2.coffee/
# https://atmospherejs.com/dandv/http-more
# mongo filter for auth "334" vs 334 :(
#
#Meteor._sleepForMs(2000);
#
# bulk insert
#http://stackoverflow.com/questions/19757434/bulk-mongodb-insert-in-meteor-or-node
#http://stackoverflow.com/questions/15365747/how-to-use-mongoimport-with-my-meteor-application-database
#
#
########################### Shared Code #########################
#

AuthorityDB = new (Mongo.Collection)('authority')
EstablishmentDB = new (Mongo.Collection)('establishment')

# Setup pince logger  - https://atmospherejs.com/jag/pince
log = new Logger('app')
Logger.setLevel 'info'
Logger.setLevel 'app', 'trace'

# Experimental logger : 
# tb = Observatory.getToolbox()
# tb info "got json back ", result


#
########################### Client Code #########################
#

if Meteor.isClient

  # These helpers are the reactive hooks used by the template
  Template.foodform.helpers 
    authority: -> Session.get 'authority'
    businesses: -> Session.get 'businesses'
    summary: -> Session.get 'summary'
    authorities: -> 
      options = 
        sort: 
          name: 1
        fields: 
          name : 1
          localAuthorityId : 1 

      cursor = AuthorityDB.find {}, options

      return cursor

  # when the user changes the authority
  onChangeAuthority = (evt) ->
    newValue = $(evt.target).val()
    Session.set 'authority', newValue

    log.info("setting localAuthorityId to #{newValue}")

    # note we HAVE to cast to a Number - lost a lot of time on that! :(
    selector =
      localAuthorityId : Number(newValue)

    cursor = EstablishmentDB.find selector
    businesses = cursor.fetch()

    #log.trace businesses
    Session.set 'businesses', businesses

    updateSummaryData businesses

    return

  # TODO: This is slow as does three passes through businesses array. Could do this on server if issue
  # TODO: Use a data table https://atmospherejs.com/aslagle/reactive-table
  # TODO: Use a data table http://reactive-table.meteor.com/
  # TODO: Use a data table https://atmospherejs.com/ephemer/reactive-datatables

  updateSummaryData = (businesses) ->
    sumMap = {}

    #TODO handle empty select case
    #TODO change to normal for loop

    # init all to zero
    sumMap[business.ratingValue] = 0 for business in businesses
    # increment each one
    sumMap[business.ratingValue] += 1 for business in businesses

    total = 0
    total += count for ratingType, count of sumMap


    # is this authority using 1-5 scheme?
    hasPass = false
    hasPass |= business.ratingValue.indexOf('Pass') != -1 for business in businesses

    log.trace sumMap

    # if using the 1-5 scheme
    if !hasPass 
      log.trace "using a 1-5 scheme, padding out"

      # TODO: Confirm
      # ensure Exempt
      if !sumMap['Exempt']
        sumMap['Excempt'] = 0;

      # ensure we include zero entries for 1-5
      for i in [1..5]
        if !sumMap[i]
          sumMap[i] = 0;

    if total == 0
      total = 1

    log.trace sumMap
    log.trace total

    summary = []
    for key, value of sumMap
      summary.push key: key, value: Math.round(value*100/total) + '%'

    Session.set 'summary', summary


  # register event handlers for foodform
  Template.foodform.events 
    'change #authorityPicker': onChangeAuthority




#
######################### Server code #########################
#

if Meteor.isServer

  #TODO:Clear DB
  Kadira.connect('5FgBP5BXun7muycFp', 'e78ff442-ce3b-4883-a89f-83608afa052a')

  #
  # Food Standards API Constants
  #

  # URLS
  apiUrlAuthorities = 'http://api.ratings.food.gov.uk/Authorities/basic'
  apiUrlAuthoritiesPageSize = 50

  apiUrlEstablishments = 'http://api.ratings.food.gov.uk/Establishments'

  # request headers
  apiHeaders =
    'x-api-version' : '2'
    'accept' : 'application/json'
    'content-type' : 'application/json'
    'Accept-Language' : 'en-GB'

  # Collection Upsert option
  dbUpsertOption = upsert: true

  # # create a method to return authority names
  # Meteor.methods
  #   getAuthorityNames: () ->
  #     log.info "Fetching authority names"
  #     cursor = AuthorityDB.find {}, { fields: 'name' : 1 }
  #     log.trace "cursor: " + cursor.fetch()
  #     return cursor.fetch()

  updateEstablishmentsDB = (authorityId, authorityDescription, establishments) ->

    #log.trace establishments
    log.info "Updating for #{establishments.length} establishments"

    for establishment in establishments
      # capture establishment in scope - the coffescript way :/
      do (establishment) ->
        establishmentLogLabel = "#{establishment.BusinessName} (#{establishment.FHRSID})"

        log.trace "Upserting establishment (for #{authorityId}) : #{establishmentLogLabel}"

        # TODO: Confirm FHRSID is GUID
        selector = 
          FHRSID: establishment.FHRSID
      
        setter = $set: 
          FHRSID: establishment.FHRSID
          localAuthorityId: authorityId
          ratingValue: establishment.RatingValue
          businessName: establishment.BusinessName

        EstablishmentDB.update selector, setter, dbUpsertOption, 
          (error, numberOfInserts) ->
            if !error
              log.trace "Upserted establishment (for #{authorityId}): #{establishmentLogLabel}"
            else
              log.error "Couldn't upsert establishment (for #{authorityId}): #{establishmentLogLabel}"

    return



  updateEstablishments = (authority, authorityLogLabel, pageNumber) ->

    log.info "Getting establishments for #{authorityLogLabel} - page #{pageNumber}"
    
    log.trace authority

    query = 
      'localAuthorityId' : authority.LocalAuthorityId
      'pageSize' : 500
      'pageNumber' : pageNumber

    options = 
      'headers': apiHeaders
      'params': query

    HTTP.get apiUrlEstablishments, options, 
      (error, result) ->
        log.debug "pageSize #{query.pageSize}"
        if !error
          #log.debug result.content.meta.pageSize
          parsedContent = JSON.parse(result.content)

          log.debug "url was  : " + result.href
          log.debug "pageSize from server : #{parsedContent.meta.pageSize}" 
          log.debug "currentPage from server : #{parsedContent.meta.pageSize}"

          # update DB
          updateEstablishmentsDB authority.LocalAuthorityId, authorityLogLabel, parsedContent.establishments

          # is this the last page?
          if parsedContent.meta.totalPages == pageNumber
            log.info "Processed all establishment pages for #{authorityLogLabel}"
            return
          else
            # # limit to one page for now - TODO: open up
            # return

            # get next page
            pageNumber++
            log.debug "Getting next page #{pageNumber}"
            updateEstablishments authority, authorityLogLabel, pageNumber

        else
          log.error "Couldn't get establishments for authority: #{authorityLogLabel}"
    

    return
      
  
  # upsert the authority to DB 
  updateAuthorityDB = (authorities, lazyUpdate) ->
    log.info "Updating for #{authorities.length} authorities"

    for authority, i in authorities
      authorityLogLabel = "#{authority.Name} (#{authority.LocalAuthorityId})"

      selector = localAuthorityId: authority.LocalAuthorityId
      cursor = EstablishmentDB.find selector, {}

      if cursor.fetch().length > 0 && lazyUpdate
        log.debug "lazyUpdate so skipping authority that already has captured establishments : #{authority.Name}"
        continue;

      setter = $set: 
        localAuthorityId: authority.LocalAuthorityId
        name: authority.Name

      log.debug "Upserting authority #{authorityLogLabel}"

      # limit scope to this loop - the coffescript way :/
      do (authority, authorityLogLabel) ->
        # upsert this Authority
        AuthorityDB.update selector, setter, dbUpsertOption, 
          (error, numberOfInserts) ->
            if !error
              log.debug "Upserted authority #{authorityLogLabel}"
              # now go update the establishments for this authority
              updateEstablishments authority, authorityLogLabel, 1
            else
              log.error "Couldn't upsert authority: #{authorityLogLabel}"


    log.info "Updating for all Authorities complete"

    return


  refreshDb = (pageNumber) ->

    HTTP.get "#{apiUrlAuthorities}/#{pageNumber}/#{apiUrlAuthoritiesPageSize}", { headers: apiHeaders }, (error, result) ->
      if !error
        # parse the content
        parsedContent = JSON.parse(result.content)
        authorities = parsedContent.authorities

        log.info "Fetched #{authorities.length} authorities from remote API : #{apiUrlAuthorities}"

        # update the db with authorities in a lazy fashion
        updateAuthorityDB authorities, true

        # is this the last page?
        if parsedContent.meta.totalPages == pageNumber
          log.info "Updated for all authorities"
          return
        else
          # get next page
          pageNumber++
          log.debug "Getting next page #{pageNumber}"
          refreshDb pageNumber

      else
        log.error "getAuthorities failed with HTTP error: #{error}"
      return

  # run on server at startup
  Meteor.startup ->
    # refresh db from page 1
    refreshDb(1)

    # TODO: Create into a method and trigger from admin panel 

    # HTTP GET Authorities

    return

