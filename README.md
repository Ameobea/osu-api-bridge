# `osu-api-bridge`

This application serves as an API bridge for [osu!track](https://ameobea.me/osutrack/).  It makes requests to the new v2 OAuth osu! API and returns responses in a normalized format for osu!track's legacy needs.

It started out as a very minimal single-endpoint service, but I've added other endpoints over time to support other projects like my [osu! Beatmap Atlas](https://osu-map.ameo.design/)

Some of this functionality requires access to a private database.  If you want to run this service yourself, you'll want to disable that functionality.  You can do that by disabling default features when compiling this binary.
