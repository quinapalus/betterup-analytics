# A class to convert a parsed JSON resume from Sovren into a row to add to a
# pandas DataFrame. Conversion is initiated with "extract_attributes".
# Only stores the most recent row "extracted".


class JSONtoCSV:

    headers = [
        "ResumeSource",
        "FileName",
        "FormattedName",
        "FirstName",
        "LastName",
        "CountryCode",
        "Municipality",
        "YearsOfExperience",
        "ExperienceSummary",
        "NumberOfEmployers",
        "EmployerNames",
        "EducationDetail",
        "Certifications",
        "Languages",
    ]

    def __init__(self):
        self.newRow = {}

    def get_headers(self):
        return self.headers

    def get_row(self):
        return self.newRow

    def get_clean_row(self):
        row = self.get_row()
        scrubbedRow = {}
        for k, v in row.items():
            if k in [
                "FormattedName",
                "FirstName",
                "LastName",
                "Municipality",
                "EmployerNames",
                "EducationDetail",
            ] and isinstance(v, str):
                scrubbedRow[k] = v.title()
            else:
                scrubbedRow[k] = v
        return scrubbedRow

    def append(self, k, v):
        self.newRow[k] = v

    # @arg resume: JSON output from Sovren parser
    # @desc: Extract key attributes from JSON file and transform into ResumeRow.
    # Each call overwrites prior row stored in Resume Row(0)
    def extract_attributes(self, resume, resumeSource, fileName):
        self.newRow = {}

        self.append("ResumeSource", resumeSource)
        self.append("FileName", fileName)

        structuredXMLresume = resume["Resume"]["StructuredXMLResume"]

        # Extract Contact Info
        contactInfo = structuredXMLresume.get("ContactInfo")
        self.extract_contact_info(contactInfo)

        # Extract Experience Summary
        userArea = resume["Resume"]["UserArea"]["sov:ResumeUserArea"]
        experienceSummary = userArea.get("sov:ExperienceSummary")
        self.extract_experience_summary(experienceSummary)

        # Extract Employment History
        employmentHistory = structuredXMLresume.get("EmploymentHistory")
        self.extract_employment_history(employmentHistory)

        # Extract Education History
        educationHistory = structuredXMLresume.get("EducationHistory")
        self.extract_education_history(educationHistory)

        # Extract Professional Certifications
        certifications = structuredXMLresume.get("LicensesAndCertifications")
        self.extract_certifications(certifications)

        # Extract Spoken Languages
        languages = structuredXMLresume.get("Languages")
        self.extract_languages(languages)

        # Extract Professional Associations
        """
        # Not used - Associations data is of poor quality
        """
        # associations = structuredXMLresume.get("Associations")
        # self.extract_associations(associations)

    # Extract name and location
    def extract_contact_info(self, contactInfo):
        if contactInfo:
            # Extract name
            nameInfo = contactInfo.get("PersonName")
            if nameInfo:
                self.append("FormattedName", nameInfo.get("FormattedName"))
                self.append("FirstName", nameInfo.get("GivenName"))
                self.append("LastName", nameInfo.get("FamilyName"))
            # Extract Location
            contactMethods = contactInfo.get("ContactMethod")
            if contactMethods:
                for method in contactMethods:
                    if "PostalAddress" in method:
                        address = method.get("PostalAddress")
                        self.append("CountryCode", address.get("CountryCode"))
                        self.append("Municipality", address.get("Municipality"))

    # Extract years of work experience and skills taxonomy output
    def extract_experience_summary(self, experienceSummary):
        if experienceSummary:
            monthsExperience = int(experienceSummary.get("sov:MonthsOfWorkExperience"))
            self.append("YearsOfExperience", round(monthsExperience / 12, 1))
            skillsTaxonomy = experienceSummary.get("sov:SkillsTaxonomyOutput")
            if skillsTaxonomy:
                skills = []
                for skill in skillsTaxonomy.get("sov:TaxonomyRoot")[0].get(
                    "sov:Taxonomy"
                ):
                    name = skill.get("@name")
                    percent = skill.get("@percentOfOverall")
                    if int(percent) >= 10:
                        skills.append(name + ": " + percent)
                self.append("ExperienceSummary", ", ".join(skills))

    # Extract number of employers and names of employers
    def extract_employment_history(self, employmentHistory):
        if employmentHistory:
            # Extract number of employers
            self.append("NumberOfEmployers", len(employmentHistory.get("EmployerOrg")))
            # Extract employers
            employers = []
            for employer in employmentHistory.get("EmployerOrg"):
                employers.append(employer.get("EmployerOrgName"))
            self.append("EmployerNames", ", ".join(employers))

    # Extract institutions attended and degrees earned
    def extract_education_history(self, educationHistory):
        if educationHistory:
            # Extract name of institution, name of degree and date of degree
            degrees = []
            for institution in educationHistory.get("SchoolOrInstitution"):
                institutionName = institution.get("School", [{}])[0].get(
                    "SchoolName", ""
                )
                for degree in institution.get("Degree"):
                    majors = []
                    for major in degree.get("DegreeMajor", []):
                        majors.append(major.get("Name")[0])
                    degreeMajors = ", ".join(majors)
                    degreeName = degree.get("DegreeName", "")
                    degreeDate = degree.get("DegreeDate", {}).get("YearMonth", "")
                    degrees.append(
                        ", ".join(
                            [institutionName, degreeMajors, degreeName, degreeDate]
                        )
                    )
            self.append("EducationDetail", "\n".join(degrees))

    def extract_certifications(self, certifications):
        if certifications:
            certs = []
            for certification in certifications.get("LicenseOrCertification"):
                certs.append(certification.get("Name"))
            self.append("Certifications", ", ".join(certs))

    def extract_languages(self, languages):
        if languages:
            langs = []
            for language in languages.get("Language"):
                langs.append(language.get("LanguageCode"))
            self.append("Languages", ", ".join(langs))

    def extract_associations(self, associations):
        if associations:
            assoc = []
            for association in associations.get("Association"):
                assoc.append(association.get("Name"))
            self.append("Associations", ", ".join(assoc))
