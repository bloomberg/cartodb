namespace :profiles do
  desc 'Enable a user for a profile by username'
  task :enable_user, [:username, :profile_name] => [:environment] do |task, args|
    raise 'Please specify the username to be enabled' if args[:username].blank?
    raise "Please specify the profile_name for which to enable #{args[:username]}" if args[:profile_name].blank?

    user = User.where(username: args[:username]).first
    raise "The username '#{args[:username]}' does not correspond to any user" if user.nil?

    profile = Profile.where(name: args[:profile_name]).first
    raise "The profile_name '#{args[:profile_name]}' does not correspond to any profile" if profile.nil?

    profiles_user = ProfilesUser.where(profile_id: profile.id, user_id: user.id).first
    if profiles_user
      puts "User #{user.username} already enabled for profile #{profile.name} - skipping."
    else
      ProfilesUser.insert(profile_id: profile.id, user_id: user.id)
    end
  end

  desc 'Disable a user for a profile by username'
  task :disable_user, [:username, :profile_name] => [:environment] do |task, args|
    raise 'Please specify the username to be disabled' if args[:username].blank?
    raise "Please specify the profile_name for which to disable #{args[:username]}" if args[:profile_name].blank?

    user = User.where(username: args[:username]).first
    raise "The username '#{args[:username]}' does not correspond to any user" if user.nil?

    profile = Profile.where(name: args[:profile_name]).first
    raise "The profile_name '#{args[:profile_name]}' does not correspond to any profile" if profile.nil?

    profiles_user = ProfilesUser.where(profile_id: profile.id, user_id: user.id).first
    if !profiles_user
      puts "User #{user.username} not enabled for profile #{profile.name} - skipping."
    else
      profiles_user.destroy
    end
  end
end

