{% docs fact_manager_feedback_request_status %} 

    Manager feedback request status model used to track feedback status to accompany 
    the model used in manager feedback flows.  It will focus on looking at replacing 
    the model used in Mode (to be deprecated in Dec.2022) and 
    the Manager Feedback Request Status dashboard. 

{% enddocs %}


{% docs fact_manager_feedback_request_status__primary_key %}

    Surrogate key using dbt_utils.surrogate_key.  Utilizing 'member_id', 'manager_id', , 'track_assignment_id', 'feedback_request_assessment_id'

{% enddocs %}

{% docs fact_manager_feedback_request_status__member_id %}

	Unique identifier for the member. 
	
{% enddocs %}


{% docs fact_manager_feedback_request_status__manager_id %}

    Unique identifier for the manager.

{% enddocs %}


{% docs fact_manager_feedback_request_status__track_assignment_id %}

    Unique identifier for the member track assignment. 

{% enddocs %}


{% docs fact_manager_feedback_request_status__feedback_request_assessment_id %}

    Unique identifier for the feedback assessment of type 'Assessments::ManagerFeedbackRequestAssessment'

{% enddocs %}


{% docs fact_manager_feedback_request_status__manager_feedback_assessment_id %}

    Unique identifier for the feedback assessment of type 'Assessments::ManagerFeedbackAssessment'

{% enddocs %}


{% docs fact_manager_feedback_request_status__feedback_request_submitted_at %}

    Time the assessment is submitted at of the 'Assessments::ManagerFeedbackRequestAssessment' type.  

{% enddocs %}


{% docs fact_manager_feedback_request_status__manager_feedback_assessment_submitted_at %}

    Time the assessment is submitted at of the 'Assessments::ManagerFeedbackAssessment' type. 

{% enddocs %}


{% docs fact_manager_feedback_request_status__first_completed_session_at %}

    Time the member completed their first session.

{% enddocs %}


{% docs fact_manager_feedback_request_status__days_from_first_session_to_member_request %}

    Number of days from member first session to member request of manager feedback assessment. 

{% enddocs %}


{% docs fact_manager_feedback_request_status__days_from_first_session_to_manager_submission %}

    Number of days from member first session to the manager feedback assessment submission.  

{% enddocs %}


{% docs fact_manager_feedback_request_status__days_since_request_unanswered %}

    Number of days from member request to current date, given that the request is unanswered. 

{% enddocs %}


{% docs fact_manager_feedback_request_status__days_since_request_answered %}

    Number of days from member request to the time requested assessments is submitted.

{% enddocs %}
