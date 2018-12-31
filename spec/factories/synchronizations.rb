FactoryGirl.define do
  factory :remote_synchronization, class: Carto::Synchronization do
    id  { random_uuid }
    state 'success'
    service_name 'connector'
  end
end
