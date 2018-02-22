# Be sure to restart your server when you modify this file.

# Add new mime types for use in respond_to blocks:
# Mime::Type.register "text/richtext", :rtf
# Mime::Type.register_alias "text/html", :iphone
Mime::Type.register "application/vnd.google-earth.kml+xml", :kml
Mime::Type.register "application/vnd.google-earth.kml+xml", :kmz
Mime::Type.register "application/octet-stream", :shp # sent as zip
Mime::Type.register "application/octet-stream", :sql # sent as zip
Mime::Type.register "image/svg+xml", :svg
# Below is required for serving .carto files locally (ie: no riak).
# Without this Samples Save As will fail on import due to incorrectly identifying file as different type.
Rack::Mime::MIME_TYPES['.carto'] = 'application/zip'
