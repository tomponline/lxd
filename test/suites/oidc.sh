test_oidc() {
  # shellcheck disable=2153
  ensure_has_localhost_remote "${LXD_ADDR}"

  # Check OIDC scopes validation
  ! lxc config set oidc.scopes "my-scope" || false # Doesn't contain "email" or "openid"
  ! lxc config set oidc.scopes "my-scope email" || false # Doesn't contain "openid"
  ! lxc config set oidc.scopes "my-scope openid" || false # Doesn't contain "email"

  lxc config set oidc.scopes "my-scope email openid" # Valid
  lxc config unset oidc.scopes # Should reset to include profile and offline access claims

  # Setup OIDC
  spawn_oidc

  # Ensure a clean state before testing validation & rollback
  lxc config unset oidc.issuer
  lxc config unset oidc.client.id

  # Should be failed on wrong issuer.
  ! lxc config set oidc.issuer="http://127.0.0.1:22/" oidc.client.id="device" || false # Wrong port
  ! lxc config set "oidc.issuer=http://127.0.0.1:$(< "${TEST_DIR}/oidc.port")/wrong-path" "oidc.client.id=device" || false # Invalid path
  ! lxc config set "oidc.issuer=https://idp.example.com/" "oidc.client.id=device" || false # Invalid host


  # Should remain empty as above tests failed.
  [ -z "$(lxc config get oidc.client.id || echo fail)" ]
  [ -z "$(lxc config get oidc.issuer || echo fail)" ]

  lxc config set "oidc.issuer=http://127.0.0.1:$(< "${TEST_DIR}/oidc.port")/" "oidc.client.id=device" # Valid Configuration

  # Expect this to fail. No user set.
  ! BROWSER=curl lxc remote add --accept-certificate oidc "${LXD_ADDR}" --auth-type oidc || false

  # Set a user with no email address
  set_oidc test-user

  # Expect this to fail. mini-oidc will issue a token but adding the remote will fail because no email address will be
  # returned from /userinfo
  ! BROWSER=curl lxc remote add --accept-certificate oidc "${LXD_ADDR}" --auth-type oidc || false

  # Set a user with an email address
  set_oidc test-user test-user@example.com

  # This should succeed.
  BROWSER=curl lxc remote add --accept-certificate oidc "${LXD_ADDR}" --auth-type oidc

  # The user should now be logged in and their email should show in the "auth_user_name" field.
  lxc query oidc:/1.0 | jq --exit-status '.auth == "trusted"'
  lxc query oidc:/1.0 | jq --exit-status '.auth_user_name == "test-user@example.com"'

  # OIDC user should be added to identities table.
  [ "$(lxd sql global --format csv "SELECT COUNT(*) FROM identities WHERE type = 5 AND identifier = 'test-user@example.com' AND auth_method = 2")" = 1 ]

  # Cleanup OIDC
  lxc auth identity delete oidc/test-user@example.com
  lxc remote remove oidc
  lxc config set oidc.issuer="" oidc.client.id=""
  kill_oidc
}
