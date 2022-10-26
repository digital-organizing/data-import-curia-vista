#!/usr/bin/env python3
import argparse
import logging
import sys
import traceback

from odata2sql import command_dot, command_dump, command_init, command_sync, command_benchmark_aiohttp, \
    command_benchmark_parallel
from odata2sql.odata import Context, SettingsBuilder

log = logging.getLogger('curia_vista')

"""Settings format easy to understand for humans """
SYNC_CONFIGURATION = {
    'sync_unconfigured_entities': True,
    'entities': {
        'Bill': {
            'selected_properties': {
                'ID',
                'Language',
                # 'IdBusiness',
                'BusinessNumber',
                # 'BusinessShortNumber',
                # 'BusinessType',
                # 'BusinessTypeName',
                # 'BusinessTypeAbbreviation',
                'Title',
                'BillNumber',
                'BillType',
                # 'BillTypeName',
                # 'BusinessStatus',
                # 'BusinessStatusText',
                # 'BusinessStatusDate',
                'Modified',
                'SubmissionDate',
            },
        },
        'BillLink': {
            'selected_properties': {
                'ID',
                'Language',
                'IdBill',
                'LinkText',
                'LinkUrl',
                'Modified',
                'LinkTypeId',
                'LinkType',
                'StartDate',
            },
        },
        'BillStatus': {
            'selected_properties': {
                'ID',
                'Language',
                'IdBill',
                'Status',
                'StatusText',
                'Council',
                # 'CouncilName',
                # 'CouncilAbbreviation',
                'Category',
                'CategoryName',
                # 'CommitteeType',
                # 'CommitteeName',
                # 'CommitteeAbbreviation',
                # 'CommitteeAbbreviation1',
                # 'CommitteeAbbreviation2',
                'Modified',
            },
        },
        'Business': {
            'selected_properties': {
                'ID',
                'Language',
                'BusinessShortNumber',
                'BusinessType',
                'BusinessTypeName',
                'BusinessTypeAbbreviation',
                'Title',
                'Description',
                'InitialSituation',
                'Proceedings',
                'DraftText',
                'SubmittedText',
                'ReasonText',
                'DocumentationText',
                'MotionText',
                'FederalCouncilResponseText',
                'FederalCouncilProposal',
                'FederalCouncilProposalText',
                'FederalCouncilProposalDate',
                'SubmittedBy',
                'BusinessStatus',
                # 'BusinessStatusText',
                # 'BusinessStatusDate',
                # 'ResponsibleDepartment',
                # 'ResponsibleDepartmentName',
                # 'ResponsibleDepartmentAbbreviation',
                # 'IsLeadingDepartment',
                # 'Tags',
                # 'Category',
                # 'Modified',
                'SubmissionDate',
                'SubmissionCouncil',
                # 'SubmissionCouncilName',
                # 'SubmissionCouncilAbbreviation',
                'SubmissionSession',
                'SubmissionLegislativePeriod',
                'FirstCouncil1',
                # 'FirstCouncil1Name',
                # 'FirstCouncil1Abbreviation',
                'FirstCouncil2',
                # 'FirstCouncil2Name',
                # 'FirstCouncil2Abbreviation',
                # 'TagNames',
            },
        },
        'BusinessResponsibility': {
            'selected_properties': {
                'ID',
                'Language',
                'BusinessNumber',
                'DepartmentNumber',
                # 'DepartmentName',
                # 'DepartmentAbbreviation',
                'IsLeading',
                'Modified',
                'BillNumber',
            },
        },
        'BusinessRole': {
            'selected_properties': {
                'ID',
                'Language',
                'Role',
                'RoleName',
                'BusinessNumber',
                'IdExternal',
                'ParlGroupNumber',
                'CantonNumber',
                'CommitteeNumber',
                'MemberCouncilNumber',
                'ReturnType',
                'Modified',
                # 'BusinessShortNumber',
                # 'BusinessTitle',
                # 'BusinessSubmissionDate',
                'BusinessType',
                # 'BusinessTypeName',
                # 'BusinessTypeAbbreviation',
            },
        },
        'BusinessStatus': {
            'selected_properties': {
                'ID',
                'Language',
                'BusinessNumber',
                'BusinessStatusId',
                'BusinessStatusName',
                'BusinessStatusDate',
                'IsMotionInSecondCouncil',
                'NewKey',
                'Modified',
            },
        },
        'BusinessType': {
            'selected_properties': {
                'ID',
                'BusinessTypeName',
                'BusinessTypeAbbreviation',
                'Language',
                'Modified',
            },
        },
        'Canton': {
            'selected_properties': {
                'ID',
                'Language',
                'CantonNumber',
                'CantonName',
                'CantonAbbreviation',
            },
        },
        'Citizenship': {
            'selected_properties': {
                'ID',
                'Language',
                'PersonNumber',
                'PostCode',
                'City',
                # 'CantonName',
                'CantonAbbreviation',
                'Modified',
            },
        },
        'Committee': {
            'selected_properties': {
                'ID',
                'Language',
                'CommitteeNumber',
                'MainCommitteeNumber',
                'SubCommitteeNumber',
                'CommitteeName',
                'Abbreviation',
                'Abbreviation1',
                'Abbreviation2',
                'Council',
                # 'CouncilName',
                'Modified',
                'CommitteeType',
                'CommitteeTypeName',
                'CommitteeTypeAbbreviation',
                # 'CouncilAbbreviation',
                'DisplayType',
            },
        },
        'Council': {
            'selected_properties': {
                'ID',
                'Language',
                'CouncilName',
                'CouncilAbbreviation',
                'Modified',
            },
        },
        'External': {
            'selected_properties': {
                'ID',
                'Language',
                'Name',
                'Modified',
            },
        },
        'LegislativePeriod': {
            'selected_properties': {
                'ID',
                'Language',
                'LegislativePeriodNumber',
                'LegislativePeriodName',
                'LegislativePeriodAbbreviation',
                'StartDate',
                'EndDate',
                'Modified',
            },
        },
        'Meeting': {
            'selected_properties': {
                'ID',
                'Language',
                'MeetingNumber',
                'IdSession',
                # 'SessionNumber',
                # 'SessionName',
                'Council',
                # 'CouncilName',
                # 'CouncilAbbreviation',
                'Date',
                'Begin',
                'Modified',
                # 'LegislativePeriodNumber',
                'PublicationStatus',
                'MeetingOrderText',
                'SortOrder',
                'Location',
            },
        },
        'MemberCommittee': {
            'selected_properties': {
                'ID',
                'Language',
                'CommitteeNumber',
                'PersonNumber',
                # 'PersonIdCode',
                'CommitteeFunction',
                'CommitteeFunctionName',
                # 'FirstName',
                # 'LastName',
                'GenderAsString',  # Makes no sense, should be nullable!
                # 'PartyNumber',
                # 'PartyName',
                'Council',
                # 'CouncilName',
                'Canton',
                # 'CantonName',
                'Modified',
                'ParlGroupNumber',
                # 'ParlGroupName',
                # 'ParlGroupAbbreviation',
                # 'ParlGroupCode',
                # 'PartyAbbreviation',
                # 'CouncilAbbreviation',
                # 'CantonAbbreviation',
                # 'CommitteeName',
                # 'Abbreviation',
                # 'Abbreviation1',
                # 'Abbreviation2',
                'CommitteeType',
                'CommitteeTypeName',
                # 'CommitteeTypeAbbreviation',
            },
        },
        'MemberCommitteeHistory': {  # Relevant: Join and leave date
            'selected_properties': {
                'ID',
                'Language',
                'PersonNumber',
                # 'PersonIdCode',
                # 'FirstName',
                # 'LastName',
                'GenderAsString',  # Makes no sense, should be nullable!
                'CommitteeNumber',
                # 'CommitteeName',
                # 'Abbreviation',
                # 'Abbreviation1',
                # 'Abbreviation2',
                # 'CommitteeFunction',
                # 'CommitteeFunctionName',
                'DateJoining',
                'DateLeaving',
                'ParlGroupNumber',
                # 'ParlGroupName',
                # 'ParlGroupAbbreviation',
                # 'ParlGroupCode',
                # 'PartyNumber',
                # 'PartyName',
                # 'PartyAbbreviation',
                'Council',
                # 'CouncilName',
                # 'CouncilAbbreviation',
                'Canton',
                # 'CantonName',
                # 'CantonAbbreviation',
                'Modified',
            },
        },
        'MemberCouncil': {
            'selected_properties': {
                'ID',
                'Language',
                'IdPredecessor',
                'PersonNumber',
                # 'PersonIdCode',
                'Active',
                # 'FirstName',
                # 'LastName',
                'GenderAsString',
                'Canton',  # Makes no sense, should be nullable!
                # 'CantonName',
                # 'CantonAbbreviation',
                'Council',
                # 'CouncilName',
                # 'CouncilAbbreviation',
                'ParlGroupNumber',
                # 'ParlGroupName',
                # 'ParlGroupAbbreviation',
                # 'ParlGroupFunction',
                # 'ParlGroupFunctionText',
                'Party',
                # 'PartyName',
                # 'PartyAbbreviation',
                # 'MilitaryRank',
                # 'MilitaryRankText',
                # 'MaritalStatus',
                # 'MaritalStatusText',
                # 'BirthPlace_City',
                # 'BirthPlace_Canton',
                'Mandates',
                'AdditionalMandate',
                'AdditionalActivity',
                # 'OfficialName',
                'DateJoining',
                'DateLeaving',
                'DateElection',
                'DateOath',
                'DateResignation',
                'Modified',
                # 'NumberOfChildren',
                # 'Citizenship',
                # 'DateOfBirth',
                # 'DateOfDeath',
            },
        },
        'MemberCouncilHistory': {
            'selected_properties': {
                'ID',
                'Language',
                # 'IdPredecessor',
                'PersonNumber',
                # 'PersonIdCode',
                # 'Active',
                # 'FirstName',
                # 'LastName',
                'GenderAsString',  # Makes no sense, should be nullable!
                # 'Canton',
                # 'CantonName',
                # 'CantonAbbreviation',
                # 'Council',
                # 'CouncilName',
                # 'CouncilAbbreviation',
                # 'ParlGroupNumber',
                # 'ParlGroupName',
                # 'ParlGroupAbbreviation',
                # 'ParlGroupFunction',
                # 'ParlGroupFunctionText',
                # 'Party',
                # 'PartyName',
                # 'PartyAbbreviation',
                # 'MilitaryRank',
                # 'MilitaryRankText',
                # 'MaritalStatus',
                # 'MaritalStatusText',
                # 'BirthPlace_City',
                # 'BirthPlace_Canton',
                # 'Mandates',
                # 'AdditionalMandate',
                # 'AdditionalActivity',
                # 'OfficialName',
                'DateJoining',
                'DateLeaving',
                'DateElection',
                'DateOath',
                'DateResignation',
                'Modified',
                # 'Citizenship',
                # 'DateOfBirth',
                # 'DateOfDeath',
            },
        },
        'MemberParlGroup': {
            'selected_properties': {
                'ID',
                'Language',
                'PersonNumber',
                # 'PersonIdCode',
                # 'FirstName',
                # 'LastName',
                # 'OfficialName',
                'GenderAsString',  # Makes no sense, should be nullable!
                'PartyNumber',
                # 'PartyName',
                # 'PartyAbbreviation',
                'CantonNumber',
                # 'CantonName',
                # 'CantonAbbreviation',
                'ParlGroupNumber',
                # 'ParlGroupName',
                # 'ParlGroupAbbreviation',
                # 'ParlGroupFunction',
                # 'ParlGroupFunctionName',
                'CouncilNumber',
                # 'CouncilName',
                # 'CouncilAbbreviation',
                'Modified',
            },
        },
        'MemberParty': {
            'selected_properties': {
                'ID',
                'Language',
                'PartyNumber',
                # 'PartyName',
                'PersonNumber',
                # 'PersonIdCode',
                # 'FirstName',
                # 'LastName',
                'GenderAsString',
                # 'PartyFunction',
                'Modified',
                # 'PartyAbbreviation',
            },
        },
        'Objective': {
            'selected_properties': {
                'ID',
                'Language',
                'PublicationDate',
                'ReferenceType',
                # 'ReferenceTypeName',
                'ReferenceText',
                'PublicationType',
                # 'PublicationTypeName',
                # 'PublicationText',
                # 'PublicationVolume',
                # 'PublicationYear',
                # 'PublicationNumber',
                # 'IsOldPublicationFormat',
                'Modified',
                'IdBusiness',
                'IdBill',
                'BusinessNumber',
                # 'BusinessShortNumber',
                'BillNumber',
                'BusinessType',
                # 'BusinessTypeName',
                # 'BusinessTypeAbbreviation',
                # 'PublicationTypeAbbreviation',
                'ReferendumDeadline',
            },
        },
        'ParlGroup': {
            'selected_properties': {
                'ID',
                'Language',
                'ParlGroupNumber',
                'IsActive',
                'ParlGroupCode',
                'ParlGroupName',
                'ParlGroupAbbreviation',
                'NameUsedSince',
                'Modified',
                'ParlGroupColour',
            },
        },  # Useless because ParlGroupHistory exists
        'ParlGroupHistory': {
            'selected_properties': {
                'ID',
                'Language',
                'ParlGroupNumber',
                'ParlGroupColour',
                'IsActive',
                'ParlGroupName',
                'ParlGroupAbbreviation',
                'NameUsedSince',
                'Modified',
            },
        },
        'Party': {
            'selected_properties': {
                'ID',
                'Language',
                'PartyNumber',
                # 'PartyName',
                'StartDate',
                'EndDate',
                'Modified',
                'PartyAbbreviation',
            },
        },
        'Person': {
            'selected_properties': {
                'ID',
                'Language',
                'PersonNumber',
                'PersonIdCode',
                'Title',
                'TitleText',
                'LastName',
                'GenderAsString',
                'DateOfBirth',
                'DateOfDeath',
                'MaritalStatus',
                'MaritalStatusText',
                'PlaceOfBirthCity',
                'PlaceOfBirthCanton',
                'Modified',
                'FirstName',
                'OfficialName',
                'MilitaryRank',
                'MilitaryRankText',
                'NativeLanguage',
                'NumberOfChildren',
            },
        },
        'PersonAddress': {
            'selected_properties': {
                'ID',
                'Language',
                'Modified',
                'PersonNumber',
                'AddressType',
                'AddressTypeName',
                'IsPublic',
                'AddressLine1',
                'AddressLine2',
                'AddressLine3',
                'City',
                # 'CantonName',
                'Comments',
                'CantonNumber',
                'Postcode',
                # 'CantonAbbreviation',
            },
        },
        'PersonCommunication': {
            'selected_properties': {
                'ID',
                'Language',
                'PersonNumber',
                'Address',
                'CommunicationType',
                'CommunicationTypeText',
                'Modified',
            },
        },
        'PersonEmployee': {
            'selected_properties': {
                'ID',
                'Language',
                'PersonNumber',
                'LastName',
                'FirstName',
                'Employer',
                'JobTitle',
                'Modified',
            },
        },
        'PersonInterest': {
            'selected_properties': {
                'ID',
                'Language',
                'OrganizationType',
                'OrganizationTypeText',
                'OrganizationTypeShortText',
                'FunctionInAgency',
                'FunctionInAgencyText',
                'FunctionInAgencyShortText',
                'Agency',
                'PersonNumber',
                'Modified',
                'InterestType',
                'InterestTypeText',
                'InterestTypeShortText',
                # 'FirstName',
                # 'LastName',
                'InterestName',
                'SortOrder',
                'Paid',
            },
        },
        'PersonOccupation': {
            'selected_properties': {
                'ID',
                'Language',
                'PersonNumber',
                'Occupation',
                'OccupationName',
                'StartDate',
                'EndDate',
                'Modified',
                'Employer',
                'JobTitle',
            },
        },
        'Preconsultation': {
            'selected_properties': {
                'ID',
                'Language',
                'IdBill',
                # 'BillNumber',
                'BusinessNumber',
                # 'BusinessShortNumber',
                # 'BusinessTitle',
                'CommitteeNumber',
                # 'CommitteeName',
                'CommitteeDisplayType',
                # 'Abbreviation',
                # 'Abbreviation1',
                # 'Abbreviation2',
                'PreconsultationDate',
                'TreatmentCategory',
                'Modified',
                'BusinessType',
                # 'BusinessTypeName',
                # 'BusinessTypeAbbreviation',
            },
        },
        'Publication': {
            'selected_properties': {
                'ID',
                'Language',
                'PublicationType',
                'PublicationTypeName',
                'SortOrder',
                'IsOldFormat',
                'Title',
                'Page',
                'Volume',
                'Year',
                'Modified',
                'BusinessNumber',
                # 'BusinessShortNumber',
                'PublicationTypeAbbreviation',
            },
        },
        'Rapporteur': {
            'selected_properties': {
                'ID',
                'BusinessNumber',
                # 'BusinessShortNumber',
                # 'BusinessTitle',
                'IdBill',
                'CommitteeNumber',
                'MemberCouncilNumber',
                # 'LastName',
                # 'FirstName',
                'Language',
                'Modified',
            },
        },
        'RelatedBusiness': {
            'selected_properties': {
                'ID',
                'Language',
                'BusinessNumber',
                # 'BusinessTitle',
                # 'BusinessShortNumber',
                'RelatedBusinessNumber',
                # 'RelatedBusinessShortNumber',
                # 'RelatedBusinessTitle',
                'PriorityCode',
                'Modified',
                'RelatedBusinessType',
                # 'RelatedBusinessTypeName',
                # 'RelatedBusinessTypeAbbreviation',
            },
        },
        'Resolution': {
            'selected_properties': {
                'ID',
                'Language',
                'ResolutionNumber',
                'ResolutionDate',
                'ResolutionId',
                'ResolutionText',
                'Council',
                # 'CouncilName',
                'Category',
                # 'CategoryName',
                # 'CommitteeType',
                # 'CommitteeName',
                'Modified',
                'IdBill',
                # 'CouncilAbbreviation',
                # 'CommitteeAbbreviation',
                # 'CommitteeAbbreviation1',
                # 'CommitteeAbbreviation2',
                'Committee',
            },
        },
        'SeatOrganisationNr': {
            'selected_properties': {
                'ID',
                'SeatNumber',
                'PersonNumber',
                # 'PersonIdCode',
                # 'FirstName',
                # 'LastName',
                # 'CantonAbbreviation',
                'ParlGroupNumber',
                # 'ParlGroupName',
                'Language',
            },
        },
        'Session': {
            'selected_properties': {
                'ID',
                'Language',
                'SessionNumber',
                'SessionName',
                'Abbreviation',
                'StartDate',
                'EndDate',
                'Title',
                'Type',
                'TypeName',
                'Modified',
                'LegislativePeriodNumber',
            },
        },
        'Subject': {
            'selected_properties': {
                'ID',
                'Language',
                'IdMeeting',
                'VerbalixOid',
                'SortOrder',
                'Modified',
            },
        },
        'SubjectBusiness': {
            'selected_properties': {
                'IdSubject',
                'BusinessNumber',
                'Language',
                # 'BusinessShortNumber',
                'Title',
                'SortOrder',
                'PublishedNotes',
                'Modified',
                'TitleDE',
                'TitleFR',
                'TitleIT',
            },
        },
        'Tags': {
            'selected_properties': {
                'ID',
                'Language',
                'TagName',
            },
        },
        'Transcript': {
            'selected_properties': {
                'ID',
                'Language',
                'IdSubject',
                'VoteId',
                'PersonNumber',
                'Type',
                'Text',
                'MeetingCouncilAbbreviation',
                'MeetingDate',
                'MeetingVerbalixOid',
                'IdSession',
                'SpeakerFirstName',
                'SpeakerLastName',
                'SpeakerFullName',
                'SpeakerFunction',
                'CouncilId',
                # 'CouncilName',
                'CantonId',
                # 'CantonName',
                # 'CantonAbbreviation',
                'ParlGroupName',
                'ParlGroupAbbreviation',
                'SortOrder',
                'Start',
                'End',
                'Function',
                'DisplaySpeaker',
                'LanguageOfText',
                'Modified',
                'StartTimeWithTimezone',
                'EndTimeWithTimezone',
                # 'VoteBusinessNumber',
                # 'VoteBusinessShortNumber',
                # 'VoteBusinessTitle',
            },
        },
        'Vote': {
            'selected_properties': {
                'ID',
                'Language',
                'RegistrationNumber',
                'BusinessNumber',
                # 'BusinessShortNumber',
                # 'BusinessTitle',
                # 'BusinessAuthor',
                'BillNumber',
                # 'BillTitle',
                'IdLegislativePeriod',
                # 'IdSession',
                # 'SessionName',
                'Subject',
                'MeaningYes',
                'MeaningNo',
                'VoteEnd',
                'VoteEndWithTimezone',
            },
        },
        'Voting': {
            'sync_by': 'Vote',
            'selected_properties': {
                'ID',
                'Language',
                'IdVote',
                'RegistrationNumber',
                'PersonNumber',
                # 'FirstName',
                # 'LastName',
                # 'Canton',
                # 'CantonName',
                # 'ParlGroupCode',
                # 'ParlGroupColour',
                # 'ParlGroupName',
                # 'ParlGroupNameAbbreviation',
                'Decision',
                # 'DecisionText',
                # 'BusinessNumber',
                # 'BusinessShortNumber',
                # 'BusinessTitle',
                # 'BillTitle',
                # 'IdLegislativePeriod',
                # 'IdSession',
                # 'VoteEnd',
                # 'MeaningYes',
                # 'MeaningNo',
                # 'CantonID',
                # 'Subject',
                # 'VoteEndWithTimezone',
            },
        },
    }
}


def settings_from_args(args):
    """ Massage user provided arguments into a Settings object. """
    if args.requests_cache:
        import requests_cache
        requests_cache.install_cache(args.requests_cache)

    settings_builder = SettingsBuilder(args.url)

    try:
        # Install global filter
        if len(args.language) > 1:
            filter_ = ' or '.join(f"(Language eq '{language}')" for language in args.language)
        else:
            filter_ = f"Language eq '{args.language[0]}'"
        SYNC_CONFIGURATION['filter'] = filter_
    except (TypeError, AttributeError):
        pass

    try:
        # Install individual filters
        if len(args.legislative_period) > 1:
            filter_ = ' or '.join(f"(IdLegislativePeriod eq {lp})" for lp in args.legislative_period)
        else:
            filter_ = f"IdLegislativePeriod eq {args.legislative_period[0]}"
        SYNC_CONFIGURATION['entities']['Vote']['filter'] = filter_
        SYNC_CONFIGURATION['entities']['Voting']['filter'] = filter_
    except (TypeError, AttributeError):
        pass

    try:
        for sync_by_fk_string in args.sync_by_fk:
            try:
                dependant, principal = sync_by_fk_string.split()
                SYNC_CONFIGURATION['entities'][dependant]['sync_by'] = principal
            except Exception as e:
                raise RuntimeError(f'Invalid foreign key specification: {sync_by_fk_string}')
    except (AttributeError, TypeError):
        pass

    try:
        for k in SYNC_CONFIGURATION['entities']:
            if 'sync' in SYNC_CONFIGURATION['entities'][k]:
                del SYNC_CONFIGURATION['entities'][k]['sync']
        for entity_type_name in args.include:
            SYNC_CONFIGURATION['entities'][entity_type_name]['sync'] = True
        SYNC_CONFIGURATION['sync_unconfigured_entities'] = False
    except (AttributeError, TypeError):
        pass

    try:
        for entity_type_name in args.skip:
            SYNC_CONFIGURATION['entities'][entity_type_name]['sync'] = False
    except (AttributeError, TypeError):
        pass

    try:
        settings_builder.odata_server_max_connections(args.connections)
    except AttributeError:
        pass

    return settings_builder.sync_config(SYNC_CONFIGURATION).build()


def main():
    parser_top_level = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_top_level.add_argument('-v', '--verbose',
                                  dest='verbose_count',
                                  action='count',
                                  default=0,
                                  help='Increase log verbosity for each occurrence.')
    parser_top_level.add_argument('--url', type=str, default='https://ws.parlament.ch/odata.svc')
    parser_top_level.add_argument('--requests-cache', type=str,
                                  help="Cache HTTP requests in <cache>.sqlite. Useful to speed up development.")
    subparsers = parser_top_level.add_subparsers(dest='command', required=True)
    benchmark_aiohttp_parser = subparsers.add_parser('benchmark-aiohttp', help='Benchmark OData server using aiohttp')
    benchmark_multithreading_parser = subparsers.add_parser('benchmark-multithreading',
                                                            help='Benchmark OData fetching using multiple threads')
    benchmark_multiprocessing_parser = subparsers.add_parser('benchmark-multiprocessing',
                                                             help='Benchmark OData fetching using multiple processes')
    init_parser = subparsers.add_parser('init', help='Initialize database')
    sync_parser = subparsers.add_parser('sync', help='Synchronize database from scratch')
    update_parser = subparsers.add_parser('update', help='Incrementally update database')
    dot_parser = subparsers.add_parser('dot', help='Show dependencies between entity types')
    dump_parser = subparsers.add_parser('dump', help='Show dependencies between entity types')
    for parser in [benchmark_aiohttp_parser, benchmark_multithreading_parser, benchmark_multiprocessing_parser,
                   init_parser, sync_parser, update_parser, dot_parser,
                   dump_parser]:
        parser.add_argument('--include', type=str, nargs='+',
                            help='Entity types to work on, dependencies added as needed. All if unspecified.')
    for parser in [benchmark_aiohttp_parser, benchmark_multithreading_parser, benchmark_multiprocessing_parser,
                   sync_parser, update_parser, dump_parser]:
        parser.add_argument('--skip', type=str, nargs='+', help='Forcefully ignore the listed entities. Beware!')
    for parser in [init_parser, sync_parser, update_parser]:
        parser.add_argument("-u", '--user', type=str, default='curiavista', help='Database user (default: %(default)s)')
        parser.add_argument("-H", '--host', type=str, default='127.0.0.1',
                            help='PostgreSQL host (default: %(default)s)')
        parser.add_argument("-P", '--port', type=int, default=5432, help='Port to connect on (default: %(default)s)')
        parser.add_argument("-p", '--password', type=str, help='Attempting ~/.pgpass if not provided')
        parser.add_argument("-d", '--database', dest='dbname', type=str, default='curiavista',
                            help='Database name (default: %(default)s)')
    for parser in [benchmark_aiohttp_parser, benchmark_multithreading_parser, benchmark_multiprocessing_parser,
                   sync_parser, update_parser]:
        parser.add_argument('--language', type=str, nargs='+',
                            help='Restrict import to specified language(s): DE, FR, IT, RM, EN. (default: all)')
    for parser in [sync_parser]:
        parser.add_argument('--sync-by-fk', type=str, nargs='+', action='extend',
                            help='Entity types to sync via foreign key (default: %(default)s)',
                            metavar='<Dependant Principal>')
        parser.add_argument('-j', '--connections', type=int, default=20, metavar='count',
                            help='Number of parallel HTTP connections to establish (default: %(default)s)')
        parser.add_argument('--legislative-period', type=int, nargs='+',
                            help='Legislature periods to import. All if unspecified.')
    for parser in [init_parser]:
        parser.add_argument("-f", '--force', action='store_true', help='Erase all preexisting content in database')
    for parser in [dump_parser]:
        parser.add_argument('--ipython', action='store_true', help='Drop into IPython shell')
    args = parser_top_level.parse_args()

    log_level = max(3 - args.verbose_count, 0) * 10
    log.info(f"Setting loglevel to {log_level}")
    logging.basicConfig(stream=sys.stderr, level=log_level, format='%(asctime)s %(name)s %(levelname)s %(message)s')

    context = Context.from_settings(settings_from_args(args))
    log.info(f"This is session {context.session_id}: {context.settings}")
    if args.command == 'benchmark-aiohttp':
        command_benchmark_aiohttp.work(context, args)
    if args.command == 'benchmark-multithreading':
        command_benchmark_parallel.work(context, args, 'multithreading')
    if args.command == 'benchmark-multiprocessing':
        command_benchmark_parallel.work(context, args, 'multiprocessing')
    if args.command == 'dump':
        command_dump.work(context, args)
    elif args.command == 'dot':
        command_dot.work(context, args)
    if args.command == 'init':
        command_init.work(context, args)
    if args.command == 'sync':
        command_sync.work(context, args)
    if args.command == 'update':
        raise NotImplemented('Update functionality is not yet implemented!')


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.info('Keyboard interrupt detected. Exiting...')
    except Exception as e:
        log.error(f'Exception raised: {e}')
        traceback.print_exc()
        sys.exit(1)
    sys.exit()
