{% docs int_coach__coach_profiles__staffable_states %}

The Staffable State should express, at a high level, whether a coach is able to be staffed, and why they are not able to be staffed if not. This table describes all the possible states:

| state                    | description                                                                                     | example                                                                                                               |
|--------------------------|-------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| Staffable                | Only state in which a coach is eligible to be staffed                                           |                                                                                                                       |
| Onboarding               | Not staffable - a coach is not ready to be staffable yet, still going through the hiring funnel | Coach account has been created until after final checklist review in Fountain                                         |
| Hold - voluntary         | Not staffable - a coach has reached out and requested to not receive new members                | Emergency leave, long vacation                                                                                        |
| Hold - involuntary       | Not staffable - BetterUp has chosen to put a coach on hold from receiving new members           | Quality issue, Out of compliance with training, Out of compliance with RP                                             |
| Offboarded - voluntary   | Not staffable - a coach has reached out to let us know they will no longer be a BetterUp coach  | Drops out of onboarding (inactive for 60 days), Active coach chooses to leave to take on full time position elsewhere |
| Offboarded - involuntary | Not staffable - a coach has been left go and will no longer be a BetterUp coach                 | Let go due to quality issue, etc.                                                                                     |

{% enddocs %}