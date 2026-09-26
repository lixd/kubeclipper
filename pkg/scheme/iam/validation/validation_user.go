/*
 *
 *  * Copyright 2021 KubeClipper Authors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package validation

import (
	"fmt"

	"github.com/dlclark/regexp2"

	"github.com/kubeclipper/kubeclipper/pkg/scheme/core/validation"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/iam/v1"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

var ValidateUserName = apimachineryvalidation.NameIsDNSSubdomain

func ValidateUser(u *corev1.User) field.ErrorList {
	allErrs := validation.ValidateObjectMeta(&u.ObjectMeta, false, ValidateUserName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateUserSpec(&u.Spec, field.NewPath("spec"))...)
	return allErrs
}

// PasswordPolicy is the account password policy. It is the same rule the
// platform already applies to the admin initial password (R21): 8-16
// characters with at least one digit, one lower-case and one upper-case
// letter. Go's regexp has no lookahead, so regexp2 does the matching.
const PasswordPolicy = `^(?=.*\d)(?=.*[a-z])(?=.*[A-Z]).{8,16}$`

// ValidatePassword reports whether a plaintext password satisfies
// PasswordPolicy. User creation and password changes used to accept anything
// non-empty while the admin password already had to satisfy the policy (R28).
func ValidatePassword(password string) error {
	reg, err := regexp2.Compile(PasswordPolicy, 0)
	if err != nil {
		return err
	}
	ok, err := reg.MatchString(password)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("password must be 8-16 characters and contain at least one digit, one lower-case and one upper-case letter")
	}
	return nil
}

func ValidateUserSpec(spec *corev1.UserSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	//if spec.Email == "" {
	//	allErrs = append(allErrs, field.Invalid(fldPath.Child("email"), spec.Email, "must be valid email address"))
	//}
	if spec.EncryptedPassword == "" {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("password"), spec.EncryptedPassword, "must be valid password"))
	} else if err := ValidatePassword(spec.EncryptedPassword); err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("password"), "", err.Error()))
	}
	// TODO: validate user other field
	return allErrs
}
