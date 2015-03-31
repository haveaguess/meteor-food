# Performance monitoring : https://kadira.io/
# watching DDP msgs : https://github.com/arunoda/meteor-ddp-analyzer
# DDP: Distributed Data Protocol: 
#   https://github.com/meteor/meteor/blob/devel/packages/ddp/DDP.md
#   https://meteorhacks.com/introduction-to-ddp.html
# heavy use of http://js2.coffee/

#
########################### Shared Code #########################
#

AuthorityDB = new (Mongo.Collection)('authority')
EstablishmentDB = new (Mongo.Collection)('establishment')


#
########################### Client Code #########################
#

if Meteor.isClient
  # counter starts at 0
  Session.setDefault 'counter', 0

  Template.hello.helpers counter: ->
    Session.get 'counter'

  Template.hello.events 'click button': ->
    # increment the counter when button is clicked
    Session.set 'counter', Session.get('counter') + 1
    return


#
######################### Server code #########################
#

if Meteor.isServer

  Kadira.connect('5FgBP5BXun7muycFp', 'e78ff442-ce3b-4883-a89f-83608afa052a')

  # Setup pince logger  - https://atmospherejs.com/jag/pince
  log = new Logger('app')
  Logger.setLevel 'info'
  Logger.setLevel 'app', 'debug'

  # Experimental logger : 
  # var tb = Observatory.getToolbox();
  # tb.info("got json back ", result);

  #
  # Food Standards API Constants
  #

  # URLS
  apiUrlAuthorities = 'http://api.ratings.food.gov.uk/Authorities/basic'
  apiUrlEstablishments = 'http://api.ratings.food.gov.uk/Establishments/basic'

  # request headers
  apiHeaders =
    'x-api-version' : '2'
    'accept' : 'application/json'
    'content-type' : 'application/json'
    'Accept-Language' : 'en-GB'

  # Collection Upsert option
  dbUpsertOption = upsert: true

  updateEstablishmentsDB = (establishments) ->

    #log.trace establishments
    log.info "Updating for " + establishments.length + " establishments"

    for establishment in establishments
      establishmentLogLabel = "#{establishment.BusinessName} (#{establishment.LocalAuthorityBusinessID})"

      log.debug "Upserting establishment : " + establishmentLogLabel

      selector = localAuthorityId: establishment.LocalAuthorityBusinessID
      
      setter = $set: 
        ratingValue: establishment.RatingValue
        businessName: establishment.BusinessName

      # limit scope to this loop - the coffescript way :/
      do (establishmentLogLabel) ->
        EstablishmentDB.update selector, setter, dbUpsertOption, 
          (error, numberOfInserts) ->
            if !error
              log.debug "Upserted establishment : " + establishmentLogLabel
            else
              log.error "Couldn't upsert establishment: " + establishmentLogLabel

    return



  updateEstablishments = (authority, authorityLogLabel, pageNumber) ->

    log.info "Getting establishments for #{authorityLogLabel} - page #{pageNumber}"
    here = 1
    log.info "Getting " + here++

    query = 
      'localAuthorityId' : authority.localAuthorityId
      'pageSize' : 5
      'pageNumber' : pageNumber

    options = 
      'headers': apiHeaders
      'query': query

    HTTP.get apiUrlEstablishments, options, 
      (error, result) ->
        log.info "Getting " + here++
        log.debug "pageSize " + query.pageSize
        if !error
          #log.debug result.content.meta.pageSize
          parsedContent = JSON.parse(result.content)

          log.debug "pageSize from server : " + parsedContent.meta.pageSize
          log.debug "currentPage from server : " + parsedContent.meta.pageSize

          # update DB
          updateEstablishmentsDB parsedContent.establishments

          log.info "Getting " + here++

          # is this the last page?
          if parsedContent.meta.totalPages == pageNumber
            log.info "Getting " + here++
            log.info "Processed all establishment pages for #{authorityLogLabel}"
            return
          else
            # get next page
            pageNumber++
            log.debug "Getting next page #{pageNumber}"
            updateEstablishments authority, authorityLogLabel, pageNumber

        else
          log.info "Getting " + here++
          log.error "Couldn't get establishments for authority: " + authorityLogLabel
    

    return
      
  
  # upsert the authority to DB 
  updateAuthorityDB = (authorities) ->
    log.info "Updating for " + authorities.length + " authorities"

    for authority in authorities
      authorityLogLabel = "#{authority.Name} (#{authority.LocalAuthorityId})"

      selector = localAuthorityId: authority.LocalAuthorityId
      setter = $set: 
        localAuthorityId: authority.LocalAuthorityId
        name: authority.Name

      log.debug "Upserting authority : " + authorityLogLabel

      # limit scope to this loop - the coffescript way :/
      do (authority, authorityLogLabel) ->
        # upsert this Authority
        AuthorityDB.update selector, setter, dbUpsertOption, 
          (error, numberOfInserts) ->
              if !error
                log.debug "Upserted authority : " + authorityLogLabel
                # now go update the establishments for this authority
                updateEstablishments authority, authorityLogLabel, 1
              else
                log.error "Couldn't upsert authority: " + authorityLogLabel

      break;

    log.info "Updating for all Authorities complete"

    return


  # run on server at startup
  Meteor.startup ->

    # HTTP GET Authorities
    HTTP.get apiUrlAuthorities, { headers: apiHeaders }, (error, result) ->
      if !error
        log.info 'Got authorities'
#       log.debug result.content
        
        # parse the content
        parsedContent = JSON.parse(result.content)

        # update the db with authorities
        updateAuthorityDB parsedContent.authorities

      else
        log.error 'getAuthorities failed with HTTP error: ' + error
      return

    return

