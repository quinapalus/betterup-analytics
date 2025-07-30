{% docs fact_care_coach_availability %} 

    Model to support the analysis involving coach availability.  
    Currently, we only have the totals stored in coach_availability_attributes field in the coach_recommendation_sets table.  
      
    This model focuses on answering more detailed questions such as:      
    - non-overlapping number of 30min coaching slots available to members at the time of recommendation given the top 3 coaches  
    - number of days between the recommendation slot and a given available session  
    
    Notes from developer: 
    This model that will help with an ongoing experiment.  This model is filtered for Care only, in order to avoid creating derived tables in Looker.  
    However, if it proved to be useful, it could potentially be expanded for non-Care needs, but that would involve a much bigger lift, perhaps an alternative, more involved approach 

{% enddocs %}


{% docs fact_care_coach_availability__primary_key %}

    Surrogate key using dbt_utils.surrogate_key.  Utilizing 'member_id', 'coach_id', 'coach_recommendation_set_id', 'timeslot_starts_at', 'timeslot_ends_at'

{% enddocs %}


{% docs fact_care_coach_availability__member_id %}

    Member ID.

{% enddocs %}


{% docs fact_care_coach_availability__activated_care_at %}

    Timestamp indicating when the member activated Care. 

{% enddocs %}


{% docs fact_care_coach_availability__first_completed_care_appointment_at %}

    Timestamp indicating when the member completed their first Care appointment.

{% enddocs %}


{% docs fact_care_coach_availability__days_between_activation_and_first_session %}

    Number of days between member Care activation and first Care session.

{% enddocs %}


{% docs fact_care_coach_availability__coach_id %}

    Prospective Coach ID.

{% enddocs %}


{% docs fact_care_coach_availability__coach_timeslot_available_minutes %}

    Available minutes the coach had allocated as available for the given timeslot at the time the recommendation set is created. 

{% enddocs %}


{% docs fact_care_coach_availability__half_hour_available_slot %}

    Number of half hours, 30min, slots available during the timeslot.  

{% enddocs %}


{% docs fact_care_coach_availability__days_until_available_slot %}

    Number of days between the creation of the recommendation set and the avaible timeslot. 

{% enddocs %}


{% docs fact_care_coach_availability__timeslot_starts_at %}

    Timestamp indicating when the timeslot starts at.  

{% enddocs %}


{% docs fact_care_coach_availability__timeslot_ends_at %}

    Timestamp indicating when the timeslot end at.

{% enddocs %}


{% docs fact_care_coach_availability__coach_recommendation_set_created_at %}

    Timestamp indicating when the coach recommendation set is created at. 

{% enddocs %}


{% docs fact_care_coach_availability__coach_recommendation_set_id %}

    Coach recommendation set ID.  

{% enddocs %}


{% docs fact_care_coach_availability__available_sessions_ranked %}

    Ranking of the coach available sessions to the member, ordered by session date ascending. 

{% enddocs %}


{% docs fact_care_coach_availability__days_until_first_session_available %}

    Number of days calculated from the time the recommendation set is created, to the first session available. 

{% enddocs %}
